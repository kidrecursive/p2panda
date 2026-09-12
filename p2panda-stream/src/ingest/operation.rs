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

/// Checks an incoming operation to ensure correct formatting and log integrity before persisting it
/// into the store when valid. This function is idempotent; duplicate operations are ignored.
///
/// See [`validate_operation`] for an alternative method to validate an operation without
/// persistence.
///
/// Can optionally be extended with an [`OooBuffer`] (Out-Of-Order) for offering a configurable
/// window for incoming operations to wait in memory if they can't be validated yet due to missing
/// predecessors.
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
            // Operation is in-order, process it normally.
            OooResult::InOrder(operation) => {
                check_log_and_insert(store, operation, log_id, topic, prune_flag).await?;
                IngestResult::Inserted
            }

            // Buffered operations are now in order, we process them all in bulk.
            OooResult::Ordered(operations) => {
                for operation in &operations {
                    check_log_and_insert(store, operation, log_id, topic, prune_flag).await?;
                }

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
