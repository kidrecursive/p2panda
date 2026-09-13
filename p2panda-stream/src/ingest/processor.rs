// SPDX-License-Identifier: MIT OR Apache-2.0

use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::marker::PhantomData;

use p2panda_core::traits::ShortFormat;
use p2panda_core::{AnyOperation, Extensions, Hash, LogId, Operation, SeqNum, VerifyingKey};
use p2panda_store::Transaction;
use p2panda_store::logs::LogStore;
use p2panda_store::operations::OperationStore;
use p2panda_store::topics::TopicStore;
use tokio::sync::Notify;

use crate::Processor;
use crate::ingest::args::IngestArgs;
use crate::ingest::ooo::OooBuffer;
use crate::ingest::operation::{IngestError, IngestResult, ingest_operation};
use crate::orderer::OrdererMetadata;

pub struct Ingest<S, T, L, E, TP>
where
    T: OrdererMetadata<E>,
    L: LogId,
    E: Extensions,
{
    store: S,
    // M4-05 (square-tower fork): out-of-order buffer for incoming operations, keyed by (author,
    // log_id) via `log_id` below. Previously this was never wired up in production (always
    // constructed but never reachable from `process` below, which passed `None`), so any
    // operation arriving even slightly out of its log's strict seq-num/backlink order was
    // permanently rejected with `IngestError::InvalidOperation` instead of being buffered for the
    // (usually sub-second) window until its predecessor arrived. This is reachable in practice
    // whenever more than one concurrent sync session delivers operations for the same log --
    // `p2panda-net`'s `TopicManager` does not deduplicate concurrent `Initiate`/`Accept` sessions
    // per peer (see `docs/upstream/p2panda-ingest-ooo-buffer.md`). See D3-j.
    ooo: OooBuffer<L, E>,
    // Metadata for operations currently sitting in `ooo`, keyed by operation hash. `OooBuffer`
    // only stores bare `Operation<E>` values (shared with non-`p2panda` consumers of this crate),
    // so this side table is what lets us reconstruct a full `T` (topic/source/spaces args/etc.)
    // once the buffer releases them -- the same `OrdererMetadata` pattern `Orderer` already uses
    // for its own (causal-dependency, not log-order) buffering, see `orderer/processor.rs`.
    pending_metadata: RefCell<HashMap<Hash, T::Metadata>>,
    notify: Notify,
    queue: RefCell<VecDeque<(T, IngestResult<E>)>>,
    // M4-14 round 2 (D3-r): this node's own id, purely for the `ooo_park` debug probe below.
    // `None` for every existing (test) caller of `new`, which never needed node identity; set via
    // `with_node_id` by `p2panda`'s pipeline, the only caller that has one.
    node_id: Option<VerifyingKey>,
    // square-tower fork addition (M4-22): (author, log_id) pairs for which the *known-gap* park
    // case (`OutOfOrder { no_predecessor: false }`) has already logged its one `info!` line --
    // this resting place previously logged nothing at all (unlike the `no_predecessor: true` case
    // just above, D3-r/M4-14). Cleared for a (author, log_id) once its chain actually releases
    // (an `Ordered` result for that pair), so a later, separate stall on the same log logs again.
    known_gap_park_logged: RefCell<HashSet<(VerifyingKey, L)>>,
    _marker: PhantomData<(L, TP)>,
}

impl<S, T, L, E, TP> Ingest<S, T, L, E, TP>
where
    S: Transaction
        + OperationStore<Operation<E>, Hash>
        + LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>
        + TopicStore<TP, VerifyingKey, L>,
    T: OrdererMetadata<E>,
    L: LogId,
    E: Extensions,
{
    pub fn new(store: S) -> Self {
        Self {
            store,
            ooo: OooBuffer::new(),
            pending_metadata: RefCell::new(HashMap::new()),
            notify: Notify::new(),
            queue: RefCell::new(VecDeque::new()),
            node_id: None,
            known_gap_park_logged: RefCell::new(HashSet::new()),
            _marker: PhantomData,
        }
    }

    /// Sets this node's own id, purely to tag the `ooo_park` debug probe (D3-r, M4-14 round 2).
    /// Optional: an `Ingest` without a `node_id` still buffers/releases operations identically,
    /// just without the tag on that one log line.
    pub fn with_node_id(mut self, node_id: VerifyingKey) -> Self {
        self.node_id = Some(node_id);
        self
    }
}

impl<S, T, L, E, TP> Processor<T> for Ingest<S, T, L, E, TP>
where
    S: Transaction
        + OperationStore<Operation<E>, Hash>
        + LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>
        + TopicStore<TP, VerifyingKey, L>,
    T: Borrow<Operation<E>> + Borrow<IngestArgs<L, TP>> + OrdererMetadata<E>,
    L: LogId,
    E: Extensions,
{
    type Output = (T, IngestResult<E>);

    type Error = (T, IngestError);

    async fn process(&self, input: T) -> Result<(), Self::Error> {
        let operation: &Operation<E> = input.borrow();
        let args: &IngestArgs<L, TP> = input.borrow();

        let result = ingest_operation(
            &self.store,
            Some(&self.ooo),
            operation,
            &args.log_id,
            &args.topic,
            args.prune_flag,
        )
        .await;

        let result = match result {
            Ok(result) => result,
            Err(err) => {
                // Return input arguments next to error to allow mapping it back to it's source.
                return Err((input, err));
            }
        };

        match result {
            IngestResult::OutOfOrder { no_predecessor } => {
                // Not inserted yet -- stash `input`'s own metadata (topic/source/spaces args/etc,
                // whatever `T` carries) so we can rebuild a full `T` for it once `ooo` releases
                // it below. The caller's pipeline is responsible for treating an `OutOfOrder`
                // result as "no effect yet" (mirrors how a causally-`Pending` orderer result is
                // handled) -- this processor's own job is only correct buffering + eventual
                // release, not suppressing downstream effects.
                let hash = Borrow::<Operation<E>>::borrow(&input).hash;

                // M4-14 round 2 (D3-r): `no_predecessor` means this (author, log_id) has no known
                // frontier at all yet, and this isn't the log's own first operation -- this
                // resting place previously had no log line anywhere, indistinguishable from an
                // ordinary out-of-order buffering. Only the actual missing predecessor arriving
                // (via live push, sync, or repair) releases it; a later in-order arrival elsewhere
                // in the same log won't, since there's no chain to unlock.
                if no_predecessor {
                    let args: &IngestArgs<L, TP> = input.borrow();
                    tracing::debug!(
                        target: "p2panda::stream::ooo_park",
                        node_id = ?self.node_id.map(|id| id.fmt_short()),
                        op = %hash.fmt_short(),
                        author = %Borrow::<Operation<E>>::borrow(&input).header.verifying_key.fmt_short(),
                        log_id = %format!("{:?}", args.log_id).chars().take(24).collect::<String>(),
                        seq_num = %Borrow::<Operation<E>>::borrow(&input).header.seq_num,
                        "buffered with no known predecessor for its (author, log_id)"
                    );
                } else {
                    // square-tower fork addition (M4-22): the *known-gap* case -- the log's
                    // frontier is known, but this op's seq_num doesn't chain onto it, so it parks
                    // waiting for a specific missing predecessor. Previously logged nothing at all
                    // (unlike the `no_predecessor: true` case above), so a stalled log looked
                    // identical to an ordinary, momentary out-of-order buffering -- indistinguishable
                    // right up until the ooo ring's 128-entry eviction `warn!` finally fired, often
                    // minutes later (see `docs/upstream/p2panda-ingest-drop-recovery.md`). Rate-
                    // limited to once per (author, log_id) until that log's chain actually releases
                    // (cleared in the `Ordered` arm below), so a stalled log doesn't spam on every
                    // subsequent arrival while it's parked.
                    let args: &IngestArgs<L, TP> = input.borrow();
                    let author = Borrow::<Operation<E>>::borrow(&input).header.verifying_key;
                    let key = (author, args.log_id.clone());
                    let first_park = self.known_gap_park_logged.borrow_mut().insert(key);
                    if first_park {
                        tracing::info!(
                            target: "p2panda::stream::ooo_park",
                            node_id = ?self.node_id.map(|id| id.fmt_short()),
                            op = %hash.fmt_short(),
                            author = %author.fmt_short(),
                            log_id = %format!("{:?}", args.log_id).chars().take(24).collect::<String>(),
                            seq_num = %Borrow::<Operation<E>>::borrow(&input).header.seq_num,
                            expected_seq_num = %Borrow::<Operation<E>>::borrow(&input).header.seq_num.saturating_sub(1),
                            "buffered on a known gap for its (author, log_id) (first park; further \
                             parks on this log are silent until it releases)"
                        );
                    }
                }

                self.pending_metadata
                    .borrow_mut()
                    .insert(hash, input.metadata());
                self.queue
                    .borrow_mut()
                    .push_back((input, IngestResult::OutOfOrder { no_predecessor }));
            }
            IngestResult::Ordered(ref operations) => {
                // square-tower fork addition (M4-22): this (author, log_id)'s chain just
                // released -- clear its known-gap park log-once marker so a later, separate
                // stall on the same log logs again instead of staying silenced forever.
                {
                    let args: &IngestArgs<L, TP> = input.borrow();
                    let author = Borrow::<Operation<E>>::borrow(&input).header.verifying_key;
                    self.known_gap_park_logged
                        .borrow_mut()
                        .remove(&(author, args.log_id.clone()));
                }

                // Every operation in `operations` (the just-arrived one, freeing zero or more
                // previously-buffered ones, plus itself) has now been inserted by
                // `ingest_operation`/`check_log_and_insert`. Emit one queue item per operation so
                // each flows through the rest of the pipeline as its own event, exactly as if it
                // had arrived in order in the first place.
                let trigger_hash = Borrow::<Operation<E>>::borrow(&input).hash;
                let mut input = Some(input);
                for operation in operations {
                    let item = if operation.hash == trigger_hash {
                        input.take().expect("trigger operation appears once")
                    } else {
                        let meta = self
                            .pending_metadata
                            .borrow_mut()
                            .remove(&operation.hash)
                            .expect(
                                "a previously out-of-order operation must have stashed metadata",
                            );
                        T::from_operation(operation.clone(), meta)
                    };
                    self.queue
                        .borrow_mut()
                        .push_back((item, IngestResult::Ordered(operations.clone())));
                }
            }
            IngestResult::Inserted | IngestResult::AlreadyExists | IngestResult::Outdated => {
                self.queue.borrow_mut().push_back((input, result));
            }
        }

        self.notify.notify_one(); // Wake up any pending recv.

        Ok(())
    }

    async fn next(&self) -> Result<Self::Output, Self::Error> {
        loop {
            if let Some(item) = self.queue.borrow_mut().pop_front() {
                return Ok(item);
            }

            // Wait for notification that an item was added.
            self.notify.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    use futures_util::stream;
    use p2panda_core::test_utils::TestLog;
    use p2panda_core::{Operation, Topic};
    use p2panda_store::SqliteStore;
    use tokio::task;
    use tokio_stream::StreamExt;

    use crate::Processor;
    use crate::StreamLayerExt;
    use crate::ingest::args::IngestArgs;
    use crate::orderer::OrdererMetadata;

    use super::{Ingest, IngestResult};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Event {
        pub operation: Operation,
        pub args: IngestArgs<usize, Topic>,
    }

    impl Borrow<IngestArgs<usize, Topic>> for Event {
        fn borrow(&self) -> &IngestArgs<usize, Topic> {
            &self.args
        }
    }

    impl Borrow<Operation> for Event {
        fn borrow(&self) -> &Operation {
            &self.operation
        }
    }

    impl OrdererMetadata<()> for Event {
        type Metadata = IngestArgs<usize, Topic>;

        fn metadata(&self) -> Self::Metadata {
            self.args.clone()
        }

        fn from_operation(operation: Operation, meta: Self::Metadata) -> Self {
            Self {
                operation,
                args: meta,
            }
        }
    }

    #[tokio::test]
    async fn ingest_incoming_operations() {
        let log = TestLog::new();
        let local = task::LocalSet::new();

        local
            .run_until(async move {
                let store = SqliteStore::temporary().await;
                let ingest: Ingest<SqliteStore, Event, _, _, _> = Ingest::new(store);

                let operation_0 = log.operation(b"Hi", ());
                let operation_1 = log.operation(b"Ha", ());
                let operation_2 = log.operation(b"Ho", ());

                let log_id = 0;
                let topic = Topic::random();

                let mut stream = stream::iter(vec![
                    Event {
                        operation: operation_0.clone(),
                        args: IngestArgs {
                            log_id,
                            topic,
                            prune_flag: false,
                        },
                    },
                    Event {
                        operation: operation_1.clone(),
                        args: IngestArgs {
                            log_id,
                            topic,
                            prune_flag: false,
                        },
                    },
                    Event {
                        operation: operation_2.clone(),
                        args: IngestArgs {
                            log_id,
                            topic,
                            prune_flag: false,
                        },
                    },
                ])
                .layer(ingest);

                let (event, _) = stream.next().await.unwrap().unwrap();
                assert_eq!(event.operation, operation_0);

                let (event, _) = stream.next().await.unwrap().unwrap();
                assert_eq!(event.operation, operation_1);

                let (event, _) = stream.next().await.unwrap().unwrap();
                assert_eq!(event.operation, operation_2);
            })
            .await;
    }

    /// M4-05: operations arriving out of order (2 before 1) are buffered, then released in order
    /// once the gap is filled -- exercising the reconstruction path (`OrdererMetadata::
    /// from_operation`) added to make the pre-existing `OooBuffer` reachable from production code.
    #[tokio::test]
    async fn ingest_reorders_out_of_order_operations() {
        let log = TestLog::new();
        let local = task::LocalSet::new();

        local
            .run_until(async move {
                let store = SqliteStore::temporary().await;
                let ingest: Ingest<SqliteStore, Event, _, _, _> = Ingest::new(store);

                let operation_0 = log.operation(b"Hi", ());
                let operation_1 = log.operation(b"Ha", ());
                let operation_2 = log.operation(b"Ho", ());

                let log_id = 0;
                let topic = Topic::random();
                let args = IngestArgs {
                    log_id,
                    topic,
                    prune_flag: false,
                };

                // Deliver 0, then 2 (out of order -- buffered), then 1 (releases 1 *and* 2).
                let mut stream = stream::iter(vec![
                    Event {
                        operation: operation_0.clone(),
                        args: args.clone(),
                    },
                    Event {
                        operation: operation_2.clone(),
                        args: args.clone(),
                    },
                    Event {
                        operation: operation_1.clone(),
                        args: args.clone(),
                    },
                ])
                .layer(ingest);

                // operation_0 is the first operation in a brand new log: `ooo.process` always
                // routes a log's very first operation through `push_and_pop_from` too (its
                // `latest_header` is `None`), so it comes back as `Ordered([operation_0])` rather
                // than `Inserted` -- upstream's own pre-existing `ooo.rs` behavior, unrelated to
                // this fix. Either way it's inserted and forwarded as a single, ready event.
                let (event, result) = stream.next().await.unwrap().unwrap();
                assert_eq!(event.operation, operation_0);
                assert!(matches!(result, IngestResult::Ordered(_)));

                // operation_2 arrives out of order: buffered, no effect yet.
                let (event, result) = stream.next().await.unwrap().unwrap();
                assert_eq!(event.operation, operation_2);
                // D3-r: operation_0 already established the frontier at seq_num=0, so this is an
                // ordinary known-gap buffering, not "no predecessor known at all".
                assert_eq!(
                    result,
                    IngestResult::OutOfOrder {
                        no_predecessor: false
                    }
                );

                // operation_1 arrives *directly in-order* (it is never itself buffered) and must
                // still release the already-buffered operation_2 behind it -- this is exactly the
                // gap fixed in `ooo.rs`'s `InOrder` branch (`pop_chain_after`) alongside this
                // processor's own reconstruction of `operation_2`'s event.
                let (event, result) = stream.next().await.unwrap().unwrap();
                assert_eq!(event.operation, operation_1);
                assert!(matches!(result, IngestResult::Ordered(_)));

                let (event, result) = stream.next().await.unwrap().unwrap();
                assert_eq!(event.operation, operation_2);
                assert!(matches!(result, IngestResult::Ordered(_)));
            })
            .await;
    }

    /// Minimal `tracing::Subscriber` that only counts INFO-level events on a given target --
    /// enough to assert the M4-22 known-gap park log-once behaviour without pulling in a test
    /// framework crate.
    struct CountingSubscriber {
        target: &'static str,
        count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl tracing::Subscriber for CountingSubscriber {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == self.target
        }

        // Without this, tracing's global per-callsite `Interest` cache (shared process-wide,
        // across every `tracing::subscriber::set_default` guard) can permanently cache "never
        // interested" for this callsite the first time it's ever hit with no subscriber
        // installed -- silently dropping every event under this thread-local subscriber too.
        // Returning `sometimes()` forces `enabled()` to be re-checked on every event instead.
        fn register_callsite(
            &self,
            _metadata: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            tracing::subscriber::Interest::sometimes()
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            if event.metadata().target() == self.target
                && *event.metadata().level() == tracing::Level::INFO
            {
                self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    /// M4-22: the known-gap park case (`OutOfOrder { no_predecessor: false }`) must log exactly
    /// once per (author, log_id) while parked, then log again if a *later, separate* stall hits
    /// the same log after it released -- not once per park, and not silenced forever.
    #[tokio::test]
    async fn known_gap_park_logs_once_until_released() {
        let log = TestLog::new();
        let local = task::LocalSet::new();

        local
            .run_until(async move {
                let store = SqliteStore::temporary().await;
                let ingest: Ingest<SqliteStore, Event, _, _, _> = Ingest::new(store);

                // seq_num 0..=4, in that order, as produced by `TestLog`.
                let operation_0 = log.operation(b"Hi", ());
                let operation_1 = log.operation(b"Ha", ());
                let operation_2 = log.operation(b"Ho", ());
                let operation_3 = log.operation(b"He", ());
                let operation_4 = log.operation(b"Hu", ());

                let log_id = 0;
                let topic = Topic::random();
                let args = IngestArgs {
                    log_id,
                    topic,
                    prune_flag: false,
                };

                let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let _guard = tracing::subscriber::set_default(CountingSubscriber {
                    target: "p2panda::stream::ooo_park",
                    count: count.clone(),
                });

                // operation_0 establishes the frontier (seq_num=0).
                ingest
                    .process(Event {
                        operation: operation_0.clone(),
                        args: args.clone(),
                    })
                    .await
                    .unwrap();

                // operation_2 parks on a known gap (missing operation_1) -- first park, must log.
                ingest
                    .process(Event {
                        operation: operation_2.clone(),
                        args: args.clone(),
                    })
                    .await
                    .unwrap();
                assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);

                // operation_3 parks behind operation_2 on the *same* still-open gap -- must NOT
                // log again while it's still parked.
                ingest
                    .process(Event {
                        operation: operation_3.clone(),
                        args: args.clone(),
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    count.load(std::sync::atomic::Ordering::SeqCst),
                    1,
                    "a second park on the same still-open gap must not log again"
                );

                // operation_1 arrives, releasing operation_1, operation_2 and operation_3's chain
                // (frontier now at seq_num=3).
                ingest
                    .process(Event {
                        operation: operation_1.clone(),
                        args: args.clone(),
                    })
                    .await
                    .unwrap();

                // operation_4 (seq_num=4) directly follows the just-released frontier
                // (seq_num=3), so it inserts in-order rather than parking -- confirm no further
                // log line fired for it (a sanity check on the release-then-insert path, not the
                // "logs again" claim below).
                ingest
                    .process(Event {
                        operation: operation_4.clone(),
                        args: args.clone(),
                    })
                    .await
                    .unwrap();
                assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);

                // A brand new, separate stall on the SAME log after it fully released: deliver an
                // operation at seq_num=6 while seq_num=5 never arrives. Must log again -- this is
                // a fresh park, not a continuation of the first (already cleared on release).
                let _operation_5_never_sent = log.operation(b"never sent", ());
                let operation_6 = log.operation(b"Hy", ());
                ingest
                    .process(Event {
                        operation: operation_6.clone(),
                        args: args.clone(),
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    count.load(std::sync::atomic::Ordering::SeqCst),
                    2,
                    "a later, separate stall on the same (author, log_id) after release must log \
                     again, not stay silenced by the first park's marker"
                );
            })
            .await;
    }
}
