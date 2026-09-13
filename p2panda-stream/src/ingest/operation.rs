// SPDX-License-Identifier: MIT OR Apache-2.0

//! Methods to handle p2panda operations.
use p2panda_core::prune::validate_prunable_backlink;
use p2panda_core::{
    AnyHeader, AnyOperation, Extensions, Hash, LogId, Operation, SeqNum, VerifyingKey,
};
use p2panda_store::Transaction;
use p2panda_store::logs::LogStore;
use p2panda_store::operations::OperationStore;
use p2panda_store::topics::TopicStore;
use thiserror::Error;

use crate::ingest::ooo::{OooBuffer, OooResult};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestResult<E> {
    /// Validated and inserted operation into store.
    Inserted,

    /// Duplicate operation which was ignored.
    AlreadyExists,

    /// Operation freed buffered items which are now in-order.
    ///
    /// The incoming operation itself is also included in the array.
    Ordered(Vec<Operation<E>>),

    /// Out-of-order operation which was moved to internal buffer.
    OutOfOrder {
        /// `true` when this (author, log_id) had no known predecessor at all (the log's local
        /// frontier was unknown, and this isn't the log's own first operation, `seq_num == 0`) --
        /// as opposed to the ordinary case of a known frontier with a specific gap to it. This
        /// resting place was previously invisible: see `p2panda::stream::ooo_park` (D3-r, M4-14
        /// round 2).
        no_predecessor: bool,
    },

    /// Operation was from before a pruning point and was ignored.
    Outdated,
}

/// square-tower fork addition (M4-22): number of attempts `ingest_operation` makes for the whole
/// begin -> classify -> commit span before surfacing a transient store error
/// (`IngestError::StoreError`) to the caller. 1 initial attempt + 3 retries.
const INGEST_RETRY_ATTEMPTS: usize = 4;

/// square-tower fork addition (M4-22): backoff before each retry (index 0 is the delay before the
/// *first* retry, i.e. after the initial attempt fails).
const INGEST_RETRY_BACKOFF: [std::time::Duration; INGEST_RETRY_ATTEMPTS - 1] = [
    std::time::Duration::from_millis(10),
    std::time::Duration::from_millis(50),
    std::time::Duration::from_millis(250),
];

/// square-tower fork addition (M4-22): whether a store error is transient (SQLite's connection
/// briefly contended -- `SQLITE_BUSY` / "database is locked", see
/// `docs/upstream/p2panda-ingest-drop-recovery.md`) and therefore worth retrying the whole
/// begin -> classify -> commit span for, as opposed to a structural/critical failure that should
/// surface immediately.
fn is_transient_store_error(message: &str) -> bool {
    message.contains("database is locked") || message.contains("SQLITE_BUSY")
}

/// Checks an incoming operation to ensure correct formatting and log integrity before persisting it
/// into the store when valid. This function is idempotent; duplicate operations are ignored.
///
/// See [`validate_operation`] for an alternative method to validate an operation without
/// persistence.
///
/// Can optionally be extended with an [`OooBuffer`] (Out-Of-Order) for offering a configurable
/// window for incoming operations to wait in memory if they can't be validated yet due to missing
/// predecessors.
///
/// square-tower fork addition (M4-22): retries the whole begin -> classify -> commit span up to
/// `INGEST_RETRY_ATTEMPTS` times, with backoff, when the store reports a transient error
/// (`is_transient_store_error`) -- e.g. `SQLITE_BUSY`/"database is locked" from a momentarily
/// contended connection. Without this, a single transient error on `commit`/`insert_operation`
/// silently dropped the operation: every later operation on that log then parked as a
/// known-gap `OutOfOrder`, invisibly, until the ooo ring evicted it (see
/// `docs/upstream/p2panda-ingest-drop-recovery.md`).
pub async fn ingest_operation<S, L, E, TP>(
    store: &S,
    ooo: Option<&OooBuffer<L, E>>,
    // TODO: We probably want to use AnyOperation here and convert to Operation<E> in the ingest
    // processor (and not inside of this method).
    operation: &Operation<E>,
    log_id: &L,
    topic: &TP,
    prune_flag: bool,
) -> Result<IngestResult<E>, IngestError>
where
    S: Transaction
        + OperationStore<Operation<E>, Hash>
        + LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>
        + TopicStore<TP, VerifyingKey, L>,
    L: LogId,
    E: Extensions,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        match ingest_operation_attempt(store, ooo, operation, log_id, topic, prune_flag).await {
            Err(IngestError::StoreError(message)) if is_transient_store_error(&message) => {
                if attempt >= INGEST_RETRY_ATTEMPTS {
                    return Err(IngestError::StoreError(message));
                }
                let backoff = INGEST_RETRY_BACKOFF[attempt - 1];
                tracing::warn!(
                    operation_hash = %operation.hash,
                    attempt,
                    max_attempts = INGEST_RETRY_ATTEMPTS,
                    backoff_ms = backoff.as_millis(),
                    error = %message,
                    "ingest hit a transient store error, retrying",
                );
                tokio::time::sleep(backoff).await;
            }
            other => return other,
        }
    }
}

async fn ingest_operation_attempt<S, L, E, TP>(
    store: &S,
    ooo: Option<&OooBuffer<L, E>>,
    operation: &Operation<E>,
    log_id: &L,
    topic: &TP,
    prune_flag: bool,
) -> Result<IngestResult<E>, IngestError>
where
    S: Transaction
        + OperationStore<Operation<E>, Hash>
        + LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>
        + TopicStore<TP, VerifyingKey, L>,
    L: LogId,
    E: Extensions,
{
    // 1. Operation format validation
    // ==============================

    // Check if hash associated to struct ("checksum") is matching the header's digest.
    if operation.hash != operation.header.hash() {
        return Err(IngestError::HashMismatch);
    }

    // Validate operation format.
    p2panda_core::validate_operation(operation).map_err(IngestError::InvalidOperation)?;

    let permit = store
        .begin()
        .await
        .map_err(|err| IngestError::StoreError(err.to_string()))?;

    // 2. Deduplication
    // ================

    // Ignore insertion if operation already exists.
    let already_exists = store
        .has_operation_tx(&operation.hash)
        .await
        .map_err(|err| IngestError::StoreError(err.to_string()))?;

    if already_exists {
        return Ok(IngestResult::AlreadyExists);
    }

    // 3. Out-of-order buffering (optional)
    // ====================================

    // square-tower fork addition (M4-22): set only when `ooo` actually previewed a release
    // (`OooResult::Ordered`) that still needs to be applied to the real ring -- see
    // `OooBuffer::commit_release` and its call site below, after `store.commit` succeeds. `None`
    // for every other outcome (nothing to commit-release).
    let mut pending_release: Option<(&Operation<E>, Option<AnyHeader>)> = None;

    let result = if let Some(ooo) = ooo {
        // Get log frontier.
        let latest_header = store
            .get_latest_entry_tx(&operation.header.verifying_key, log_id)
            .await
            .map_err(|err| IngestError::StoreError(err.to_string()))?
            .map(|operation| operation.header);

        // Handle out-of-order operations. This gives us a bounded window of buffering
        // ooo-operations until they are in-order without rejecting them.
        match ooo
            .process(operation, latest_header.as_ref(), log_id, prune_flag)
            .await
        {
            // Operation is in-order, process it normally. No release was previewed (an empty
            // preview short-circuits to `InOrder` inside `process` itself), so there is nothing
            // to commit-release here.
            OooResult::InOrder(operation) => {
                check_log_and_insert(store, operation, log_id, topic, prune_flag).await?;
                IngestResult::Inserted
            }

            // Buffered operations are now in order, we process them all in bulk. M4-22: `ooo`
            // only *previewed* this release (the real ring is untouched) -- insert every op and
            // commit first, then apply the release for real (`pending_release`, below) only once
            // that succeeds. If the commit fails (including a transient error a caller retries),
            // the ring is exactly as it was before this attempt, so a retry re-derives the
            // identical preview instead of the preview's items being gone from both the ring and
            // the (rolled-back) store.
            OooResult::Ordered(operations) => {
                for operation in &operations {
                    check_log_and_insert(store, operation, log_id, topic, prune_flag).await?;
                }

                pending_release = Some((operation, latest_header.clone()));
                IngestResult::Ordered(operations)
            }

            OooResult::OutOfOrder => {
                // M4-14 round 2 (D3-r): distinguish "buffered, but we don't even know of any
                // predecessor for this (author, log_id) yet" (`latest_header` was `None` and this
                // isn't the log's own first operation) from the ordinary "buffered, waiting on a
                // specific known gap" case. The former previously had no log line anywhere -- an
                // op resting there looks identical to any other buffered op, but it will never be
                // released by a later in-order arrival unlocking a chain (there's no chain to
                // unlock): only the actual missing predecessor itself, arriving out of band,
                // resolves it. See `p2panda::stream::ooo_park` in `p2panda`'s pipeline/stream
                // layer, which logs this (with node_id) once the caller knows it.
                let no_predecessor = latest_header.is_none() && operation.header.seq_num > 0;
                return Ok(IngestResult::OutOfOrder { no_predecessor });
            }
            OooResult::Outdated => return Ok(IngestResult::Outdated),
        }
    } else {
        // We don't handle out-of-order operations, continue to validate and insert if operation is
        // in-order, otherwise reject it.
        check_log_and_insert(store, operation, log_id, topic, prune_flag).await?;
        IngestResult::Inserted
    };

    store
        .commit(permit)
        .await
        .map_err(|err| IngestError::StoreError(err.to_string()))?;

    // square-tower fork addition (M4-22): the store commit above just succeeded, so it's now
    // safe to apply the release `ooo` only previewed earlier -- see `pending_release` and
    // `OooBuffer::commit_release`. Must run only after `commit` returns `Ok`: on any earlier
    // error (including a caller's retry of the whole function), the ring was never touched by the
    // preview, so nothing needs undoing there.
    if let Some((operation, latest_header)) = pending_release {
        // `ooo` is always `Some` here: `pending_release` is only ever set inside the `if let
        // Some(ooo) = ooo` branch above.
        ooo.expect("pending_release only set when ooo is Some")
            .commit_release(operation, latest_header.as_ref(), log_id)
            .await;
    }

    Ok(result)
}

async fn check_log_and_insert<S, L, E, TP>(
    store: &S,
    operation: &Operation<E>,
    log_id: &L,
    topic: &TP,
    prune_flag: bool,
) -> Result<(), IngestError>
where
    S: Transaction
        + OperationStore<Operation<E>, Hash>
        + LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>
        + TopicStore<TP, VerifyingKey, L>,
    L: LogId,
    E: Extensions,
{
    // 4. Log integrity checks
    // =======================

    let latest_header = store
        .get_latest_entry_tx(&operation.header.verifying_key, log_id)
        .await
        .map_err(|err| IngestError::StoreError(err.to_string()))?
        .map(|operation| operation.header);

    // If no pruning flag is set, we expect the log to have integrity with the previously given
    // operation.
    //
    // TODO: We can remove the header Clone and Into here once we update OperationStore to use
    // AnyOperation. See issue: https://github.com/p2panda/p2panda/issues/1018.
    validate_prunable_backlink(
        latest_header.as_ref(),
        &operation.header.clone().into(),
        prune_flag,
    )
    .map_err(IngestError::InvalidOperation)?;

    // 5. Write to database
    // ====================

    // Insert operation into store and associate its log with the given topic.
    let verifying_key = operation.header.verifying_key;

    store
        .insert_operation(&operation.hash, operation, log_id)
        .await
        .map_err(|err| IngestError::StoreError(err.to_string()))?;

    <S as TopicStore<TP, VerifyingKey, L>>::associate(store, topic, &verifying_key, log_id)
        .await
        .map_err(|err| IngestError::StoreError(err.to_string()))?;

    Ok(())
}

/// Checks an incoming operation to ensure correct formatting and log integrity.
pub async fn validate_operation<S, L, E, TP>(
    store: &S,
    operation: &Operation<E>,
    log_id: &L,
    prune_flag: bool,
) -> Result<(), IngestError>
where
    S: Transaction
        + OperationStore<Operation<E>, Hash>
        + LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>
        + TopicStore<TP, VerifyingKey, L>,
    L: LogId,
    E: Extensions,
{
    // Check if hash associated to struct ("checksum") is matching the header's digest.
    if operation.hash != operation.header.hash() {
        return Err(IngestError::HashMismatch);
    }

    // Validate operation format.
    p2panda_core::validate_operation(operation).map_err(IngestError::InvalidOperation)?;

    let permit = store
        .begin()
        .await
        .map_err(|err| IngestError::StoreError(err.to_string()))?;

    let latest_header = store
        .get_latest_entry_tx(&operation.header.verifying_key, log_id)
        .await
        .map_err(|err| IngestError::StoreError(err.to_string()))?
        .map(|operation| operation.header);

    // If no pruning flag is set, we expect the log to have integrity with the previously given
    // operation.
    //
    // TODO: We can remove the header Clone and Into here once we update OperationStore to use
    // AnyOperation. See issue: https://github.com/p2panda/p2panda/issues/1018.
    let header: AnyHeader = operation.header.clone().into();
    validate_prunable_backlink(latest_header.as_ref(), &header, prune_flag)
        .map_err(IngestError::InvalidOperation)?;

    store
        .commit(permit)
        .await
        .map_err(|err| IngestError::StoreError(err.to_string()))?;

    Ok(())
}

/// Errors which can occur due to invalid operations or critical storage failures.
#[derive(Clone, Debug, PartialEq, Error)]
pub enum IngestError {
    /// Operation can not be authenticated, has broken log- or payload integrity or doesn't follow
    /// the p2panda specification.
    #[error("invalid operation: {0}")]
    InvalidOperation(#[from] p2panda_core::OperationError),

    /// Hash delivered with operation ("checksum") does not match digest of header.
    #[error("hash associated with operation does not match header digest")]
    HashMismatch,

    /// Critical storage failure occurred. This is usually a reason to panic.
    #[error("critical storage failure: {0}")]
    StoreError(String),
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use p2panda_core::test_utils::TestLog;
    use p2panda_core::{Hash, Header, Operation, SigningKey, Topic, VerifyingKey};
    use p2panda_store::SqliteStore;
    use p2panda_store::logs::LogStore;
    use p2panda_store::topics::TopicStore;

    use crate::ingest::ooo::OooBuffer;

    use super::{IngestResult, ingest_operation};

    #[tokio::test]
    async fn valid_log() {
        let store = SqliteStore::temporary().await;
        let log = TestLog::new();

        for i in 0..128 {
            let operation = log.operation(format!("{i}").as_bytes(), ());
            let result = ingest_operation(&store, None, &operation, &1, &1, false).await;
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn deduplicate_operations() {
        let store = SqliteStore::temporary().await;
        let log = TestLog::new();
        let operation = log.operation(b"same same", ());

        let result = ingest_operation(&store, None, &operation, &1, &1, false)
            .await
            .unwrap();
        std::assert_matches!(result, IngestResult::<()>::Inserted);

        // Inserting duplicates is ok and are silently ignored.
        let result = ingest_operation(&store, None, &operation, &1, &1, false)
            .await
            .unwrap();
        std::assert_matches!(result, IngestResult::AlreadyExists);
    }

    #[tokio::test]
    async fn topic_association() {
        let store = SqliteStore::temporary().await;

        let log_0 = TestLog::new();
        let log_1 = TestLog::new();
        let log_2 = TestLog::new();

        let dogs = [2; 32];
        let cats = [3; 32];

        ingest_operation(&store, None, &log_0.operation(b"Do", ()), &0, &dogs, false)
            .await
            .unwrap();

        ingest_operation(&store, None, &log_0.operation(b"Re", ()), &0, &dogs, false)
            .await
            .unwrap();

        ingest_operation(&store, None, &log_1.operation(b"Mi", ()), &1, &dogs, false)
            .await
            .unwrap();

        ingest_operation(&store, None, &log_2.operation(b"Fa", ()), &2, &cats, false)
            .await
            .unwrap();

        ingest_operation(&store, None, &log_2.operation(b"So", ()), &2, &cats, false)
            .await
            .unwrap();

        ingest_operation(&store, None, &log_2.operation(b"La", ()), &2, &cats, false)
            .await
            .unwrap();

        // Topic "dogs" contains two logs: 0 with two operations and 1 with one operation.
        let authors =
            <SqliteStore as TopicStore<[u8; 32], VerifyingKey, usize>>::resolve(&store, &dogs)
                .await
                .unwrap();
        assert_eq!(*authors.get(&log_0.author()).unwrap(), [0]);
        assert_eq!(*authors.get(&log_1.author()).unwrap(), [1]);

        let operation = store
            .get_latest_entry(&log_0.author(), &0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.header.seq_num, 1);

        // Topic "cats" contains one log: 2 with four operations.
        let authors =
            <SqliteStore as TopicStore<[u8; 32], VerifyingKey, usize>>::resolve(&store, &cats)
                .await
                .unwrap();
        assert_eq!(*authors.get(&log_2.author()).unwrap(), [2]);

        let operation = store
            .get_latest_entry(&log_2.author(), &2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.header.seq_num, 2);
    }

    #[tokio::test]
    async fn missing_prefix() {
        let store = SqliteStore::temporary().await;
        let signing_key = SigningKey::generate();

        // Create an operation which has already advanced in the log (it has a backlink and higher
        // sequence number).
        let header = Header::builder()
            // we'll be missing 11 operations between the first and this one
            .chain(12, Hash::digest(b"mock operation"))
            .build(&signing_key, ())
            .unwrap();

        let operation = Operation::from_parts(header, None);
        let result = ingest_operation(&store, None, &operation, &1, &1, false).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn ignore_outdated_pruned_operations() {
        let store = SqliteStore::temporary().await;
        let signing_key = SigningKey::generate();

        // 1. Create an advanced operation in a log which assumes that all previous operations have
        //    been pruned.
        let header = Header::builder()
            .chain(1, Hash::digest(b"mock operation"))
            .build(&signing_key, ())
            .unwrap();
        let operation = Operation::from_parts(header, None);

        let prune_flag = true; // Ingest does not do any pruning, but the flag affects validation.
        let result = ingest_operation(&store, None, &operation, &1, &1, prune_flag).await;
        assert!(result.is_ok());

        // 2. Create an operation which is from an "outdated" seq from before the log was pruned.
        let header = Header::builder().build(&signing_key, ()).unwrap();
        let operation = Operation::from_parts(header, None);

        let result = ingest_operation(&store, None, &operation, &1, &1, false).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn ooo_operation_before_pruning_point() {
        let log = TestLog::new();

        let store = SqliteStore::temporary().await;
        let ooo = OooBuffer::with_capacity(32);

        let operation_0 = log.operation(b"This is a poopy message.", ());
        let operation_1 = log.operation(b"There's nothing to see.", ());

        let log_id = 0;
        let topic = Topic::random();

        // Ingest second operation in log (seq_num=1) which has a pruning point. We expect this to
        // be a valid operation and successfully ingested.
        let result =
            ingest_operation(&store, Some(&ooo), &operation_1, &log_id, &topic, true).await;
        assert_matches!(result, Ok(IngestResult::<()>::Inserted));

        // The next operation is "from the past" (out-of-order) and already redundant due to pruning
        // of the log before.
        let result =
            ingest_operation(&store, Some(&ooo), &operation_0, &log_id, &topic, false).await;
        assert_matches!(result, Ok(IngestResult::Outdated));
    }

    #[tokio::test]
    async fn ooo_operations() {
        let log = TestLog::new();

        let store = SqliteStore::temporary().await;
        let ooo = OooBuffer::with_capacity(32);

        let operation_0 = log.operation(b"Order", ());
        let operation_1 = log.operation(b"Please", ());
        let operation_2 = log.operation(b"!", ());

        let log_id = 0;
        let topic = Topic::random();

        // D3-r: both of these arrive with an empty store (no predecessor known at all for this
        // (author, log_id) yet, and neither is the log's own seq_num=0), so both must report
        // `no_predecessor: true`.
        let result =
            ingest_operation(&store, Some(&ooo), &operation_1, &log_id, &topic, false).await;
        assert_eq!(
            result,
            Ok(IngestResult::OutOfOrder {
                no_predecessor: true
            })
        );

        let result =
            ingest_operation(&store, Some(&ooo), &operation_2, &log_id, &topic, false).await;
        assert_eq!(
            result,
            Ok(IngestResult::OutOfOrder {
                no_predecessor: true
            })
        );

        let result =
            ingest_operation(&store, Some(&ooo), &operation_0, &log_id, &topic, false).await;
        assert_eq!(
            result,
            Ok(IngestResult::Ordered(vec![
                operation_0,
                operation_1,
                operation_2
            ]))
        );
    }

    #[tokio::test]
    async fn ooo_operation_with_known_frontier_is_not_no_predecessor() {
        // D3-r: an ordinary out-of-order arrival (the log's frontier IS known, there's just a
        // specific gap to it) must report `no_predecessor: false` -- only the "we don't know of
        // any predecessor at all yet" case (`ooo_operations` above) is `true`.
        let log = TestLog::new();

        let store = SqliteStore::temporary().await;
        let ooo = OooBuffer::with_capacity(32);

        let operation_0 = log.operation(b"Order", ());
        let operation_1 = log.operation(b"Please", ());
        let operation_2 = log.operation(b"!", ());

        let log_id = 0;
        let topic = Topic::random();

        // Establish a known frontier at seq_num=0. A log's very first operation is itself routed
        // through `push_and_pop_from` (its own `latest_header` is `None`), so it comes back as
        // `Ordered([operation_0])` rather than `Inserted` -- see `ingest_reorders_out_of_order_
        // operations` in `processor.rs` for the same, pre-existing (unrelated to this fix) detail.
        let result =
            ingest_operation(&store, Some(&ooo), &operation_0, &log_id, &topic, false).await;
        assert_eq!(result, Ok(IngestResult::Ordered(vec![operation_0.clone()])));

        // Skip seq_num=1 -- the frontier is known (seq_num=0), so this is an ordinary gap, not "no
        // predecessor known at all".
        let result =
            ingest_operation(&store, Some(&ooo), &operation_2, &log_id, &topic, false).await;
        assert_eq!(
            result,
            Ok(IngestResult::OutOfOrder {
                no_predecessor: false
            })
        );

        let result =
            ingest_operation(&store, Some(&ooo), &operation_1, &log_id, &topic, false).await;
        assert_eq!(
            result,
            Ok(IngestResult::Ordered(vec![operation_1, operation_2]))
        );
    }
}

/// M4-22: `ingest_operation` retries a transient store error (`SQLITE_BUSY`/"database is locked")
/// instead of silently dropping the operation. Reuses the fault-injection `FaultyStore` shape
/// from the root-cause repro (scratchpad `refute-ooo/h1/operation.rs.with_test`): a real
/// `SqliteStore` wrapped so exactly one targeted `insert_operation` call fails, standing in for a
/// single transient store contention -- `ingest_operation` folds every store error into
/// `IngestError::StoreError` via `.to_string()`, so any genuine store error exercises the same
/// real production code path the incident hit.
#[cfg(test)]
mod m4_22_retry_tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use p2panda_core::test_utils::TestLog;
    use p2panda_core::{AnyOperation, Extensions, Hash, LogId, Operation, SeqNum, VerifyingKey};
    use p2panda_store::Transaction;
    use p2panda_store::logs::LogStore;
    use p2panda_store::operations::OperationStore;
    use p2panda_store::sqlite::{SqliteError, SqliteStore, TransactionPermit};
    use p2panda_store::topics::TopicStore;

    use super::{IngestResult, ingest_operation, ingest_operation_attempt};

    /// Wraps a real `SqliteStore`; `insert_operation` fails with a transient-looking store error
    /// ("database is locked", matching `is_transient_store_error`) for `fail_hash`, exactly
    /// `failures` times, then behaves normally -- standing in for a store that's momentarily
    /// contended and then recovers, exactly the scenario `is_transient_store_error` classifies as
    /// worth retrying.
    #[derive(Clone)]
    struct FaultyStore {
        inner: SqliteStore,
        fail_hash: Hash,
        remaining_failures: Arc<std::sync::atomic::AtomicU32>,
    }

    impl FaultyStore {
        fn new(inner: SqliteStore, fail_hash: Hash, failures: u32) -> Self {
            Self {
                inner,
                fail_hash,
                remaining_failures: Arc::new(std::sync::atomic::AtomicU32::new(failures)),
            }
        }

        fn locked_error() -> SqliteError {
            SqliteError::Sqlite(sqlx::Error::InvalidArgument("database is locked".to_string()))
        }
    }

    impl Transaction for FaultyStore {
        type Error = SqliteError;
        type Permit = TransactionPermit;

        async fn begin(&self) -> Result<Self::Permit, Self::Error> {
            self.inner.begin().await
        }

        async fn rollback(&self, permit: Self::Permit) -> Result<(), Self::Error> {
            self.inner.rollback(permit).await
        }

        async fn commit(&self, permit: Self::Permit) -> Result<(), Self::Error> {
            self.inner.commit(permit).await
        }
    }

    impl<E> OperationStore<Operation<E>, Hash> for FaultyStore
    where
        E: Extensions,
    {
        type Error = SqliteError;

        async fn insert_operation<L: LogId>(
            &self,
            id: &Hash,
            operation: &Operation<E>,
            log_id: &L,
        ) -> Result<bool, Self::Error> {
            if *id == self.fail_hash {
                let remaining = self.remaining_failures.load(Ordering::SeqCst);
                if remaining > 0
                    && self
                        .remaining_failures
                        .compare_exchange(
                            remaining,
                            remaining - 1,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                {
                    return Err(Self::locked_error());
                }
            }
            self.inner.insert_operation(id, operation, log_id).await
        }

        async fn get_operation(&self, id: &Hash) -> Result<Option<Operation<E>>, Self::Error> {
            self.inner.get_operation(id).await
        }

        async fn get_operation_tx(&self, id: &Hash) -> Result<Option<Operation<E>>, Self::Error> {
            self.inner.get_operation_tx(id).await
        }

        async fn has_operation(&self, id: &Hash) -> Result<bool, Self::Error> {
            <SqliteStore as OperationStore<Operation<E>, Hash>>::has_operation(&self.inner, id)
                .await
        }

        async fn has_operation_tx(&self, id: &Hash) -> Result<bool, Self::Error> {
            <SqliteStore as OperationStore<Operation<E>, Hash>>::has_operation_tx(&self.inner, id)
                .await
        }

        async fn delete_operation(&self, id: &Hash) -> Result<bool, Self::Error> {
            <SqliteStore as OperationStore<Operation<E>, Hash>>::delete_operation(&self.inner, id)
                .await
        }

        async fn delete_operation_payload(&self, id: &Hash) -> Result<bool, Self::Error> {
            <SqliteStore as OperationStore<Operation<E>, Hash>>::delete_operation_payload(
                &self.inner,
                id,
            )
            .await
        }
    }

    impl<L> LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash> for FaultyStore
    where
        L: LogId + Send + Sync + 'static,
    {
        type Error = SqliteError;

        async fn get_latest_entry(
            &self,
            author: &VerifyingKey,
            log_id: &L,
        ) -> Result<Option<AnyOperation>, Self::Error> {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::get_latest_entry(
                &self.inner, author, log_id,
            )
            .await
        }

        async fn get_latest_entry_tx(
            &self,
            author: &VerifyingKey,
            log_id: &L,
        ) -> Result<Option<AnyOperation>, Self::Error> {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::get_latest_entry_tx(
                &self.inner, author, log_id,
            )
            .await
        }

        async fn get_log_heights(
            &self,
            author: &VerifyingKey,
            logs: &[L],
        ) -> Result<Option<std::collections::BTreeMap<L, SeqNum>>, Self::Error> {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::get_log_heights(
                &self.inner, author, logs,
            )
            .await
        }

        async fn get_log_size(
            &self,
            author: &VerifyingKey,
            log_id: &L,
            after: Option<SeqNum>,
            until: Option<SeqNum>,
        ) -> Result<Option<(u32, u32)>, Self::Error> {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::get_log_size(
                &self.inner,
                author,
                log_id,
                after,
                until,
            )
            .await
        }

        fn log_entries(
            &self,
            author: &VerifyingKey,
            log_id: &L,
            after: Option<SeqNum>,
            until: Option<SeqNum>,
        ) -> Result<
            futures_util::stream::BoxStream<
                'static,
                Result<p2panda_store::logs::StreamItem<AnyOperation, L>, Self::Error>,
            >,
            Self::Error,
        > {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::log_entries(
                &self.inner, author, log_id, after, until,
            )
        }

        async fn prune_entries(
            &self,
            author: &VerifyingKey,
            log_id: &L,
            until: &SeqNum,
        ) -> Result<u64, Self::Error> {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::prune_entries(
                &self.inner, author, log_id, until,
            )
            .await
        }
    }

    impl<T, L> TopicStore<T, VerifyingKey, L> for FaultyStore
    where
        SqliteStore: TopicStore<T, VerifyingKey, L>,
    {
        type Error = <SqliteStore as TopicStore<T, VerifyingKey, L>>::Error;

        async fn associate(
            &self,
            topic: &T,
            author: &VerifyingKey,
            data_id: &L,
        ) -> Result<bool, Self::Error> {
            <SqliteStore as TopicStore<T, VerifyingKey, L>>::associate(
                &self.inner, topic, author, data_id,
            )
            .await
        }

        async fn remove(
            &self,
            topic: &T,
            author: &VerifyingKey,
            data_id: &L,
        ) -> Result<bool, Self::Error> {
            <SqliteStore as TopicStore<T, VerifyingKey, L>>::remove(
                &self.inner, topic, author, data_id,
            )
            .await
        }

        async fn resolve(
            &self,
            topic: &T,
        ) -> Result<std::collections::BTreeMap<VerifyingKey, Vec<L>>, Self::Error> {
            <SqliteStore as TopicStore<T, VerifyingKey, L>>::resolve(&self.inner, topic).await
        }

        async fn resolve_topics(
            &self,
            author: &VerifyingKey,
            data_id: &L,
        ) -> Result<Vec<T>, Self::Error> {
            <SqliteStore as TopicStore<T, VerifyingKey, L>>::resolve_topics(
                &self.inner, author, data_id,
            )
            .await
        }

        async fn topics(&self) -> Result<Vec<T>, Self::Error> {
            <SqliteStore as TopicStore<T, VerifyingKey, L>>::topics(&self.inner).await
        }
    }

    /// A single transient failure (mirrors one "database is locked" event on real contention):
    /// `ingest_operation`'s retry must recover, and the op ends up `Inserted`, actually persisted.
    #[tokio::test]
    async fn retries_single_transient_failure_and_inserts() {
        let inner = SqliteStore::temporary().await;
        let log = TestLog::new();
        let operation = log.operation(b"flaky write", ());

        let store = FaultyStore::new(inner.clone(), operation.hash, 1);

        let result = ingest_operation(&store, None, &operation, &1, &1, false).await;
        assert_eq!(
            result,
            Ok(IngestResult::Inserted),
            "a single transient store error must be retried into a successful Inserted, not \
             surfaced to the caller"
        );

        let persisted = OperationStore::<Operation<()>, Hash>::has_operation(&inner, &operation.hash)
            .await
            .unwrap();
        assert!(
            persisted,
            "the operation must actually be persisted after the retry recovers, not merely \
             reported as Inserted"
        );
    }

    /// Exhausts all retries (4 attempts total: `INGEST_RETRY_ATTEMPTS`): `ingest_operation` must
    /// give up and surface the store error, exactly like the pre-fix behaviour.
    #[tokio::test]
    async fn gives_up_after_exhausting_retries() {
        let inner = SqliteStore::temporary().await;
        let log = TestLog::new();
        let operation = log.operation(b"always flaky", ());

        // More failures than INGEST_RETRY_ATTEMPTS (4) -- every attempt fails.
        let store = FaultyStore::new(inner.clone(), operation.hash, 10);

        let result = ingest_operation(&store, None, &operation, &1, &1, false).await;
        assert!(
            matches!(result, Err(super::IngestError::StoreError(_))),
            "exhausting every retry must surface the store error to the caller, got {result:?}"
        );

        let persisted = OperationStore::<Operation<()>, Hash>::has_operation(&inner, &operation.hash)
            .await
            .unwrap();
        assert!(
            !persisted,
            "an operation that never got past a permanently failing store must not be persisted"
        );
    }

    /// Mutation: calling the un-retried, single-attempt `ingest_operation_attempt` directly (i.e.
    /// today's pre-fix behaviour, with the retry loop bypassed) must silently drop the operation
    /// on the very same single transient failure that `retries_single_transient_failure_and_inserts`
    /// shows the retry-wrapped `ingest_operation` recovers from -- proving this test is actually
    /// sensitive to the retry loop's presence, not to some incidental property of the fault.
    #[tokio::test]
    async fn without_retry_a_single_transient_failure_is_dropped() {
        let inner = SqliteStore::temporary().await;
        let log = TestLog::new();
        let operation = log.operation(b"flaky write, no retry", ());

        let store = FaultyStore::new(inner.clone(), operation.hash, 1);

        let result = ingest_operation_attempt(&store, None, &operation, &1, &1, false).await;
        assert!(
            matches!(result, Err(super::IngestError::StoreError(_))),
            "without the retry loop, a single transient failure must surface as a StoreError \
             (today's pre-fix, dropped-write behaviour), got {result:?}"
        );

        let persisted = OperationStore::<Operation<()>, Hash>::has_operation(&inner, &operation.hash)
            .await
            .unwrap();
        assert!(
            !persisted,
            "without the retry loop, the operation must be dropped (not persisted) -- this is \
             the M4-22 incident this fix closes"
        );
    }
}

/// M4-22 fix-round: the retry must be atomic with the ooo ring. `ingest_operation`'s ooo release
/// (`OooBuffer::process`) previously mutated the ring (drained a released chain out of it) BEFORE
/// the batch's inserts and `store.commit()`; on a transient commit error the retry re-ran
/// `ingest_operation_attempt` with only the trigger op, but the ring no longer held the rest of
/// the chain (already drained) and the DB had rolled the inserts back -- the released ops were
/// lost from both. Fixed by previewing the release without mutating the ring
/// (`OooBuffer::peek_chain_after` / `peek_push_and_pop_from`) and only draining it for real
/// (`commit_release`) after `store.commit` returns `Ok`.
#[cfg(test)]
mod m4_22_ooo_retry_atomicity_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use p2panda_core::test_utils::TestLog;
    use p2panda_core::{AnyOperation, Extensions, Hash, LogId, Operation, SeqNum, VerifyingKey};
    use p2panda_store::Transaction;
    use p2panda_store::logs::LogStore;
    use p2panda_store::operations::OperationStore;
    use p2panda_store::sqlite::{SqliteError, SqliteStore, TransactionPermit};
    use p2panda_store::topics::TopicStore;

    use crate::ingest::ooo::OooBuffer;

    use super::{IngestResult, ingest_operation};

    /// Wraps a real `SqliteStore`; `commit` fails with a transient-looking store error
    /// ("database is locked") exactly `failures` times (across the *whole* store, not scoped to
    /// one operation -- mirrors "the first commit" of a release batch failing), then behaves
    /// normally. Everything else delegates straight through.
    #[derive(Clone)]
    struct FaultyCommitStore {
        inner: SqliteStore,
        remaining_failures: Arc<AtomicU32>,
    }

    impl FaultyCommitStore {
        fn new(inner: SqliteStore, failures: u32) -> Self {
            Self {
                inner,
                remaining_failures: Arc::new(AtomicU32::new(failures)),
            }
        }
    }

    impl Transaction for FaultyCommitStore {
        type Error = SqliteError;
        type Permit = TransactionPermit;

        async fn begin(&self) -> Result<Self::Permit, Self::Error> {
            self.inner.begin().await
        }

        async fn rollback(&self, permit: Self::Permit) -> Result<(), Self::Error> {
            self.inner.rollback(permit).await
        }

        async fn commit(&self, permit: Self::Permit) -> Result<(), Self::Error> {
            let remaining = self.remaining_failures.load(Ordering::SeqCst);
            if remaining > 0
                && self
                    .remaining_failures
                    .compare_exchange(remaining, remaining - 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                // Roll back for real (mirrors what a real failed commit leaves behind: nothing
                // persisted), then report the transient error `ingest_operation` retries on.
                self.inner.rollback(permit).await.ok();
                return Err(SqliteError::Sqlite(sqlx::Error::InvalidArgument(
                    "database is locked".to_string(),
                )));
            }
            self.inner.commit(permit).await
        }
    }

    impl<E> OperationStore<Operation<E>, Hash> for FaultyCommitStore
    where
        E: Extensions,
    {
        type Error = SqliteError;

        async fn insert_operation<L: LogId>(
            &self,
            id: &Hash,
            operation: &Operation<E>,
            log_id: &L,
        ) -> Result<bool, Self::Error> {
            self.inner.insert_operation(id, operation, log_id).await
        }

        async fn get_operation(&self, id: &Hash) -> Result<Option<Operation<E>>, Self::Error> {
            self.inner.get_operation(id).await
        }

        async fn get_operation_tx(&self, id: &Hash) -> Result<Option<Operation<E>>, Self::Error> {
            self.inner.get_operation_tx(id).await
        }

        async fn has_operation(&self, id: &Hash) -> Result<bool, Self::Error> {
            <SqliteStore as OperationStore<Operation<E>, Hash>>::has_operation(&self.inner, id)
                .await
        }

        async fn has_operation_tx(&self, id: &Hash) -> Result<bool, Self::Error> {
            <SqliteStore as OperationStore<Operation<E>, Hash>>::has_operation_tx(&self.inner, id)
                .await
        }

        async fn delete_operation(&self, id: &Hash) -> Result<bool, Self::Error> {
            <SqliteStore as OperationStore<Operation<E>, Hash>>::delete_operation(&self.inner, id)
                .await
        }

        async fn delete_operation_payload(&self, id: &Hash) -> Result<bool, Self::Error> {
            <SqliteStore as OperationStore<Operation<E>, Hash>>::delete_operation_payload(
                &self.inner,
                id,
            )
            .await
        }
    }

    impl<L> LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash> for FaultyCommitStore
    where
        L: LogId + Send + Sync + 'static,
    {
        type Error = SqliteError;

        async fn get_latest_entry(
            &self,
            author: &VerifyingKey,
            log_id: &L,
        ) -> Result<Option<AnyOperation>, Self::Error> {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::get_latest_entry(
                &self.inner, author, log_id,
            )
            .await
        }

        async fn get_latest_entry_tx(
            &self,
            author: &VerifyingKey,
            log_id: &L,
        ) -> Result<Option<AnyOperation>, Self::Error> {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::get_latest_entry_tx(
                &self.inner, author, log_id,
            )
            .await
        }

        async fn get_log_heights(
            &self,
            author: &VerifyingKey,
            logs: &[L],
        ) -> Result<Option<std::collections::BTreeMap<L, SeqNum>>, Self::Error> {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::get_log_heights(
                &self.inner, author, logs,
            )
            .await
        }

        async fn get_log_size(
            &self,
            author: &VerifyingKey,
            log_id: &L,
            after: Option<SeqNum>,
            until: Option<SeqNum>,
        ) -> Result<Option<(u32, u32)>, Self::Error> {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::get_log_size(
                &self.inner,
                author,
                log_id,
                after,
                until,
            )
            .await
        }

        fn log_entries(
            &self,
            author: &VerifyingKey,
            log_id: &L,
            after: Option<SeqNum>,
            until: Option<SeqNum>,
        ) -> Result<
            futures_util::stream::BoxStream<
                'static,
                Result<p2panda_store::logs::StreamItem<AnyOperation, L>, Self::Error>,
            >,
            Self::Error,
        > {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::log_entries(
                &self.inner, author, log_id, after, until,
            )
        }

        async fn prune_entries(
            &self,
            author: &VerifyingKey,
            log_id: &L,
            until: &SeqNum,
        ) -> Result<u64, Self::Error> {
            <SqliteStore as LogStore<AnyOperation, VerifyingKey, L, SeqNum, Hash>>::prune_entries(
                &self.inner, author, log_id, until,
            )
            .await
        }
    }

    impl<T, L> TopicStore<T, VerifyingKey, L> for FaultyCommitStore
    where
        SqliteStore: TopicStore<T, VerifyingKey, L>,
    {
        type Error = <SqliteStore as TopicStore<T, VerifyingKey, L>>::Error;

        async fn associate(
            &self,
            topic: &T,
            author: &VerifyingKey,
            data_id: &L,
        ) -> Result<bool, Self::Error> {
            <SqliteStore as TopicStore<T, VerifyingKey, L>>::associate(
                &self.inner, topic, author, data_id,
            )
            .await
        }

        async fn remove(
            &self,
            topic: &T,
            author: &VerifyingKey,
            data_id: &L,
        ) -> Result<bool, Self::Error> {
            <SqliteStore as TopicStore<T, VerifyingKey, L>>::remove(
                &self.inner, topic, author, data_id,
            )
            .await
        }

        async fn resolve(
            &self,
            topic: &T,
        ) -> Result<std::collections::BTreeMap<VerifyingKey, Vec<L>>, Self::Error> {
            <SqliteStore as TopicStore<T, VerifyingKey, L>>::resolve(&self.inner, topic).await
        }

        async fn resolve_topics(
            &self,
            author: &VerifyingKey,
            data_id: &L,
        ) -> Result<Vec<T>, Self::Error> {
            <SqliteStore as TopicStore<T, VerifyingKey, L>>::resolve_topics(
                &self.inner, author, data_id,
            )
            .await
        }

        async fn topics(&self) -> Result<Vec<T>, Self::Error> {
            <SqliteStore as TopicStore<T, VerifyingKey, L>>::topics(&self.inner).await
        }
    }

    /// Parks 2 ops, delivers the trigger releasing a 3-op chain, injects ONE transient
    /// `StoreError` on the first commit -- the retry must re-derive the identical release and
    /// end up with all 3 ops persisted.
    #[tokio::test]
    async fn retry_recovers_full_release_batch_after_transient_commit_error() {
        let inner = SqliteStore::temporary().await;
        let log = TestLog::new();
        let ooo = OooBuffer::new();
        let log_id = 1;
        let topic = 1;

        // seq_num 0..=3.
        let operation_0 = log.operation(b"frontier", ());
        let operation_1 = log.operation(b"trigger", ());
        let operation_2 = log.operation(b"parked-1", ());
        let operation_3 = log.operation(b"parked-2", ());

        // Establish the frontier at seq_num=0 (no faulting yet, plain inner store).
        ingest_operation(&inner, Some(&ooo), &operation_0, &log_id, &topic, false)
            .await
            .unwrap();

        // Park operation_2 and operation_3 out of order (no store write on this path -- see
        // `OooResult::OutOfOrder` in `ooo.rs` -- so the plain inner store is fine here too).
        ingest_operation(&inner, Some(&ooo), &operation_2, &log_id, &topic, false)
            .await
            .unwrap();
        ingest_operation(&inner, Some(&ooo), &operation_3, &log_id, &topic, false)
            .await
            .unwrap();
        assert_eq!(ooo.len().await, 2, "both parked ops sit in the ring");

        // Now deliver the trigger (operation_1) through the fault-injecting store: releases
        // [operation_1, operation_2, operation_3], but the first commit attempt fails transiently.
        let faulty = FaultyCommitStore::new(inner.clone(), 1);
        let result = ingest_operation(&faulty, Some(&ooo), &operation_1, &log_id, &topic, false)
            .await
            .unwrap();
        assert!(
            matches!(result, IngestResult::Ordered(ref ops) if ops.len() == 3),
            "the retry must recover the full 3-op release, got {result:?}"
        );

        for operation in [&operation_1, &operation_2, &operation_3] {
            let persisted = OperationStore::<Operation<()>, Hash>::has_operation(
                &inner,
                &operation.hash,
            )
            .await
            .unwrap();
            assert!(
                persisted,
                "op {:?} must be persisted after the retry recovers the full release batch",
                operation.hash
            );
        }
        assert_eq!(
            ooo.len().await,
            0,
            "the ring must be fully drained of the released chain after a successful retry"
        );
    }
}
