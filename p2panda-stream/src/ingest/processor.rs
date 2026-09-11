// SPDX-License-Identifier: MIT OR Apache-2.0

use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;

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
            _marker: PhantomData,
        }
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
            IngestResult::OutOfOrder => {
                // Not inserted yet -- stash `input`'s own metadata (topic/source/spaces args/etc,
                // whatever `T` carries) so we can rebuild a full `T` for it once `ooo` releases
                // it below. The caller's pipeline is responsible for treating an `OutOfOrder`
                // result as "no effect yet" (mirrors how a causally-`Pending` orderer result is
                // handled) -- this processor's own job is only correct buffering + eventual
                // release, not suppressing downstream effects.
                let hash = Borrow::<Operation<E>>::borrow(&input).hash;
                self.pending_metadata
                    .borrow_mut()
                    .insert(hash, input.metadata());
                self.queue
                    .borrow_mut()
                    .push_back((input, IngestResult::OutOfOrder));
            }
            IngestResult::Ordered(ref operations) => {
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
                assert_eq!(result, IngestResult::OutOfOrder);

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
}
