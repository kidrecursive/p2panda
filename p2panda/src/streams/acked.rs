// SPDX-License-Identifier: MIT OR Apache-2.0

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use p2panda_core::logs::{LogHeights, LogRanges};
use p2panda_core::{AnyOperation, Cursor, Hash, SeqNum, Topic, VerifyingKey};
use p2panda_store::cursors::CursorStore;
use p2panda_store::logs::LogStore;
use p2panda_store::topics::TopicStore;
use p2panda_store::{SqliteError, SqliteStore, tx};
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore};

use crate::operation::{Header, LogId};
use crate::streams::StreamFrom;

pub type Logs = BTreeMap<VerifyingKey, Vec<LogId>>;

/// square-tower fork addition (M4-22 fix round): number of attempts `Acked::ack` makes for the
/// whole fetch-cursor->advance->persist span before surfacing a transient store error. 1 initial
/// attempt + 3 retries. Mirrors `p2panda-stream::ingest::operation::INGEST_RETRY_ATTEMPTS`.
const ACK_RETRY_ATTEMPTS: usize = 4;

/// Backoff before each retry (index 0 is the delay before the *first* retry).
const ACK_RETRY_BACKOFF: [std::time::Duration; ACK_RETRY_ATTEMPTS - 1] = [
    std::time::Duration::from_millis(10),
    std::time::Duration::from_millis(50),
    std::time::Duration::from_millis(250),
];

/// Whether a store error is transient (SQLite connection briefly contended --
/// `SQLITE_BUSY`/"database is locked") and therefore worth retrying the whole ack span for, as
/// opposed to a structural/critical failure that should surface immediately. Mirrors
/// `p2panda-stream::ingest::operation::is_transient_store_error`.
fn is_transient_store_error(err: &SqliteError) -> bool {
    let message = err.to_string();
    message.contains("database is locked") || message.contains("SQLITE_BUSY")
}

/// square-tower fork addition (M4-24): maximum number of acked operations `Acked::ack` will hold
/// in memory before flushing the advanced cursor to the store, batching what would otherwise be
/// one `BEGIN IMMEDIATE` write transaction per processed op down to at most one per this many
/// ops (or `ACK_BATCH_MAX_DELAY`, whichever comes first).
const ACK_BATCH_MAX_OPS: usize = 32;

/// Maximum time an advanced-but-unpersisted cursor is allowed to sit in memory before `ack`
/// flushes it, independent of `ACK_BATCH_MAX_OPS` -- bounds how stale a live stream's persisted
/// cursor can get relative to the in-memory one during low-throughput periods.
const ACK_BATCH_MAX_DELAY: Duration = Duration::from_millis(50);

/// In-memory batching state for `Acked::ack` (M4-24).
#[derive(Debug, Default)]
struct AckBatch {
    /// The most recently advanced cursor, not yet necessarily persisted. `None` until first
    /// loaded from the store (or reset by `replace_cursor`).
    cursor: Option<Cursor<VerifyingKey, LogId>>,
    /// Number of acks folded into `cursor` since it was last persisted.
    pending: usize,
    /// When the currently-pending (unpersisted) advance was first made; used to bound staleness
    /// by `ACK_BATCH_MAX_DELAY` even under a slow trickle of acks.
    pending_since: Option<Instant>,
}

/// Tracks a named cursor for a given topic and persists it in the store.
///
/// square-tower fork addition (M4-24): cursor persistence is batched/debounced (see
/// `AckBatch`) -- `ack` advances the in-memory cursor on every call but only writes it to the
/// store at most every `ACK_BATCH_MAX_OPS` acks or `ACK_BATCH_MAX_DELAY`, whichever comes first,
/// so a live stream costs at most one write transaction per that many ops rather than one per
/// op. The persisted cursor may therefore lag the in-memory one by at most the batch window;
/// `nacked_log_ranges`/replay always reads the persisted value, so a crash can replay up to
/// `ACK_BATCH_MAX_OPS` operations that were already (in-memory) acked -- safe, since replay is
/// idempotent (`nacked_log_ranges` diffs against actual log heights, not just the cursor). Call
/// `flush` before a graceful shutdown to persist any pending batch immediately; `replace_cursor`
/// (used to reset/replay from `StreamFrom::Start`/`Cursor`, i.e. a "nack") always writes
/// immediately and resets the batch, so a reset is never itself delayed by the batch window.
#[derive(Clone, Debug)]
pub struct Acked {
    cursor_name: String,
    topic: Topic,
    store: SqliteStore,
    semaphore: Arc<Semaphore>,
    batch: Arc<Mutex<AckBatch>>,
}

impl Acked {
    /// Creates new `Acked` instance to track ack-state using the topic as a name.
    pub fn new(store: SqliteStore, topic: impl Into<Topic>) -> Self {
        let topic = topic.into();
        Self::from_name(store, topic, topic.to_string())
    }

    /// Creates new `Acked` instance with a custom name.
    ///
    /// This is useful if we want to have multiple instances using the same topic but tracking
    /// different states.
    pub fn from_name(store: SqliteStore, topic: impl Into<Topic>, name: impl AsRef<str>) -> Self {
        Self {
            store,
            topic: topic.into(),
            cursor_name: name.as_ref().to_string(),
            semaphore: Arc::new(Semaphore::new(1)),
            batch: Arc::new(Mutex::new(AckBatch::default())),
        }
    }

    #[allow(unused)]
    pub fn cursor_name(&self) -> &str {
        &self.cursor_name
    }

    /// Returns the current acked cursor.
    ///
    /// square-tower fork addition (M4-24): prefers the in-memory batched cursor (kept
    /// up to date by every `ack`, even during the debounce window) over the store's
    /// possibly-stale persisted value, so anything that needs the "as of now" acked state within
    /// this process (`nacked_log_ranges`'s replay diffing, in particular) is never wrong merely
    /// because a batched persist hasn't fired yet -- only the on-disk value is allowed to lag.
    pub async fn cursor(&self) -> Result<Cursor<VerifyingKey, LogId>, AckedError> {
        if let Some(cursor) = self.batch.lock().await.cursor.clone() {
            return Ok(cursor);
        }
        self.persisted_cursor().await
    }

    /// Reads the cursor as currently persisted in the store, bypassing the in-memory batch.
    ///
    /// square-tower fork addition (M4-24): used by `ack_attempt` for its own store fallback
    /// (which already holds the batch lock, so it cannot go through `cursor()` without
    /// deadlocking on the same mutex).
    async fn persisted_cursor(&self) -> Result<Cursor<VerifyingKey, LogId>, AckedError> {
        let cursor = self.store.get_cursor(&self.cursor_name).await?;
        Ok(cursor.unwrap_or(Cursor::new(&self.cursor_name, LogHeights::default())))
    }

    async fn replace_cursor(
        &self,
        new_cursor: Cursor<VerifyingKey, LogId>,
    ) -> Result<Cursor<VerifyingKey, LogId>, AckedError> {
        // Fail if we try to use a cursor for a different acked state. This should help developers
        // to identify bugs.
        if new_cursor.name() != self.cursor_name {
            return Err(AckedError::InvalidName(
                new_cursor.name().to_owned(),
                self.cursor_name.to_owned(),
            ));
        }

        tx!(self.store, {
            self.store.set_cursor(&new_cursor).await?;
        });

        // square-tower fork addition (M4-24): a reset always writes immediately (above) and must
        // also reset the in-memory batch to the same value with nothing pending, so a
        // subsequently-flushed stale batch (from before the reset) can never clobber it, and
        // `ack`'s next call sees the reset cursor rather than a cached pre-reset one.
        {
            let mut batch = self.batch.lock().await;
            batch.cursor = Some(new_cursor.clone());
            batch.pending = 0;
            batch.pending_since = None;
        }

        Ok(new_cursor)
    }

    /// Returns ranges of un-acked ("nacked") events which we might want to re-play.
    pub async fn nacked_log_ranges(
        &self,
        from: StreamFrom,
    ) -> Result<LogRanges<VerifyingKey, LogId>, AckedError> {
        let _permit = self.semaphore.acquire().await;

        // Get state vector of local replica for all logs related to this topic.
        let local_log_heights = {
            let logs: Logs = self.store.resolve(&self.topic).await?;
            get_log_heights(&self.store, &logs).await?
        };

        // Get cursor with state vector of "acked" operations.
        //
        // If a new cursor was given we replace the current one with it. This changes the persisted
        // state as well and can't be reversed!
        //
        // We do this to simplify the API, otherwise we would need to keep track of two cursors
        // (one for managing the replay, another for managing the stream itself).
        let cursor = match from {
            StreamFrom::Frontier => self.cursor().await?,
            StreamFrom::Start => {
                self.replace_cursor(Cursor::new(&self.cursor_name, LogHeights::default()))
                    .await?
            }
            StreamFrom::Cursor(cursor) => self.replace_cursor(cursor).await?,
        };

        // Compute difference between local set and what was acked so far. The result is the set of
        // all not-acked operations expressed as log ranges.
        let diff = cursor.compare(&local_log_heights);

        Ok(diff)
    }

    /// Advance internal cursor by acking an operation.
    ///
    /// square-tower fork addition (M4-22 fix round): retries the whole
    /// fetch-cursor->advance->persist span up to `ACK_RETRY_ATTEMPTS` times (10/50/250ms backoff)
    /// on a transient store error (`SQLITE_BUSY`/"database is locked") before surfacing
    /// `AckedError::Store`. Without this, a single transient error here surfaced as
    /// `StreamEvent::AckFailed` instead of `Processed` (`p2panda/src/streams/stream.rs`) even
    /// though the operation itself was already durably ingested and stored -- the node's own
    /// consumers (`crates/node/src/topics.rs`) never counted the op or advanced its recorded
    /// height on `AckFailed`, matching the CI-only `retention.rs::peer_never_prunes_remote_log`
    /// failure signature (received_ops behind remote_height). Mirrors the same transient-error
    /// classifier as `p2panda-stream::ingest::operation::is_transient_store_error`.
    pub async fn ack(&self, header: impl Borrow<Header>) -> Result<(), AckedError> {
        let _permit = self.semaphore.acquire().await;

        let header = header.borrow();

        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.ack_attempt(header).await {
                Err(AckedError::Store(err)) if is_transient_store_error(&err) => {
                    if attempt >= ACK_RETRY_ATTEMPTS {
                        return Err(AckedError::Store(err));
                    }
                    let backoff = ACK_RETRY_BACKOFF[attempt - 1];
                    tracing::warn!(
                        verifying_key = %header.verifying_key,
                        seq_num = %header.seq_num,
                        attempt,
                        max_attempts = ACK_RETRY_ATTEMPTS,
                        backoff_ms = backoff.as_millis(),
                        error = %err,
                        "ack hit a transient store error, retrying",
                    );
                    tokio::time::sleep(backoff).await;
                }
                other => return other,
            }
        }
    }

    async fn ack_attempt(&self, header: &Header) -> Result<(), AckedError> {
        // square-tower fork addition (M4-24): advance the in-memory cursor and persist it only
        // every `ACK_BATCH_MAX_OPS` acks or `ACK_BATCH_MAX_DELAY`, whichever comes first, instead
        // of on every call -- see `AckBatch`/type-level docs on `Acked` for the durability
        // trade-off this implies.
        let mut batch = self.batch.lock().await;

        let mut cursor = match batch.cursor.take() {
            Some(cursor) => cursor,
            None => self.persisted_cursor().await?,
        };
        cursor.advance(
            header.verifying_key,
            header.extensions.log_id(),
            header.seq_num,
        );

        batch.pending += 1;
        let pending_since = *batch.pending_since.get_or_insert_with(Instant::now);

        let should_flush =
            batch.pending >= ACK_BATCH_MAX_OPS || pending_since.elapsed() >= ACK_BATCH_MAX_DELAY;

        // Save the advanced cursor back into the batch *before* attempting the (fallible)
        // persist below: if the write fails, the caller's retry loop (`ack`) re-enters
        // `ack_attempt` and must see this already-advanced value, not fall back to
        // `self.cursor()`'s stale persisted one and silently lose every other already-batched
        // (but not yet persisted) ack folded into it.
        batch.cursor = Some(cursor.clone());

        if should_flush {
            tx!(self.store, {
                self.store.set_cursor(&cursor).await?;
            });
            batch.pending = 0;
            batch.pending_since = None;
        }

        Ok(())
    }

    /// Persists any batched-but-unpersisted cursor advance immediately.
    ///
    /// square-tower fork addition (M4-24): call this before a graceful shutdown of the stream
    /// that owns this `Acked` (see the output-event task in `stream.rs`) so the persisted cursor
    /// never lags the in-memory one by more than `ack`'s own batch window during normal
    /// operation -- an unclean process exit can still lose the last unpersisted batch, which is
    /// the documented trade-off (D24), but a graceful shutdown must not.
    pub async fn flush(&self) -> Result<(), AckedError> {
        let mut batch = self.batch.lock().await;
        if batch.pending == 0 {
            return Ok(());
        }
        let Some(cursor) = batch.cursor.clone() else {
            return Ok(());
        };

        tx!(self.store, {
            self.store.set_cursor(&cursor).await?;
        });
        batch.pending = 0;
        batch.pending_since = None;

        Ok(())
    }
}

impl std::hash::Hash for Acked {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.cursor_name.hash(state);
    }
}

impl PartialEq for Acked {
    fn eq(&self, other: &Self) -> bool {
        self.cursor_name == other.cursor_name && self.topic == other.topic
    }
}

impl Eq for Acked {}

async fn get_log_heights(
    store: &SqliteStore,
    logs: &Logs,
) -> Result<LogHeights<VerifyingKey, LogId>, SqliteError> {
    let mut result = BTreeMap::new();

    for (verifying_key, log_ids) in logs {
        let Some(log_heights) =
            LogStore::<AnyOperation, VerifyingKey, LogId, SeqNum, Hash>::get_log_heights(
                store,
                verifying_key,
                log_ids,
            )
            .await?
        else {
            continue;
        };

        result.insert(*verifying_key, log_heights);
    }

    Ok(result)
}

/// Acknowledgment of event failed due to critical error.
#[derive(Debug, Error)]
pub enum AckedError {
    #[error("an error occurred while querying the store: {0}")]
    Store(#[from] SqliteError),

    #[error("can't use cursor with different name '{0}' for this stream, expected: {1}")]
    InvalidName(String, String),

    #[error("can't ack operation which is part of a different topic, expected: {0}")]
    InvalidTopic(Topic),
}

#[cfg(test)]
mod tests {
    use p2panda_core::{Topic, VerifyingKey};
    use p2panda_store::SqliteStore;

    use crate::Credentials;
    use crate::forge::{Forge, OperationForge};
    use crate::operation::{Extensions, LogId};
    use crate::streams::StreamFrom;

    use super::{ACK_BATCH_MAX_OPS, Acked, AckedError};

    #[tokio::test]
    async fn nacked_log_ranges() {
        let topic = Topic::random();
        let store = SqliteStore::temporary().await;
        let credentials = Credentials::generate();
        let forge = OperationForge::new(credentials, store.clone());
        let log_id = LogId::from_topic(topic);

        let acked = Acked::new(store.clone(), topic);

        // Expect name to be the same as topic.
        assert_eq!(acked.cursor_name(), topic.to_string());

        // There's nothing nacked yet.
        assert!(
            acked
                .nacked_log_ranges(StreamFrom::Frontier)
                .await
                .unwrap()
                .is_empty()
        );

        // Publish first operation.
        let operation_0 = forge
            .create_operation(
                Some(topic),
                log_id,
                Some(b"la".to_vec()),
                Extensions::from_topic(topic),
            )
            .await
            .unwrap();

        // This first operation was not acked yet.
        let ranges = acked.nacked_log_ranges(StreamFrom::Frontier).await.unwrap();
        assert_eq!(
            ranges
                .get(&forge.verifying_key())
                .unwrap()
                .get(&log_id)
                .unwrap(),
            &(None, Some(0)),
        );

        // Ack it.
        acked.ack(operation_0).await.unwrap();
        assert!(
            acked
                .nacked_log_ranges(StreamFrom::Frontier)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn custom_name() {
        let topic = Topic::random();
        let store = SqliteStore::temporary().await;
        let credentials = Credentials::generate();
        let forge = OperationForge::new(credentials, store.clone());
        let log_id = LogId::from_topic(topic);

        // We keep track of the same topic but with two independent "acked" cursors.
        let acked_1 = Acked::from_name(store.clone(), topic, "one");
        let acked_2 = Acked::from_name(store.clone(), topic, "two");

        assert_eq!(acked_1.cursor_name(), "one");
        assert_eq!(acked_2.cursor_name(), "two");

        let operation_0 = forge
            .create_operation(
                Some(topic),
                log_id,
                Some(b"la".to_vec()),
                Extensions::from_topic(topic),
            )
            .await
            .unwrap();

        // The first cursor acks it.
        acked_1.ack(operation_0).await.unwrap();

        // Both cursors end up in different states.
        let ranges_1 = acked_1
            .nacked_log_ranges(StreamFrom::Frontier)
            .await
            .unwrap();
        assert!(ranges_1.is_empty());

        let ranges_2 = acked_2
            .nacked_log_ranges(StreamFrom::Frontier)
            .await
            .unwrap();
        assert_eq!(
            ranges_2
                .get(&forge.verifying_key())
                .unwrap()
                .get(&log_id)
                .unwrap(),
            &(None, Some(0)),
        );
    }

    #[tokio::test]
    async fn replaying_mutates_cursor_state() {
        let topic = Topic::random();
        let store = SqliteStore::temporary().await;
        let credentials = Credentials::generate();
        let forge = OperationForge::new(credentials, store.clone());
        let log_id = LogId::from_topic(topic);

        let acked = Acked::new(store.clone(), topic);

        // Publish first operation and acknowledge it.
        let operation_0 = forge
            .create_operation(
                Some(topic),
                log_id,
                Some(b"la".to_vec()),
                Extensions::from_topic(topic),
            )
            .await
            .unwrap();

        acked.ack(operation_0).await.unwrap();
        assert!(
            acked
                .nacked_log_ranges(StreamFrom::Frontier)
                .await
                .unwrap()
                .is_empty()
        );

        // Requesting to stream from the start will reset the internal state.
        let ranges = acked.nacked_log_ranges(StreamFrom::Start).await.unwrap();
        assert_eq!(
            ranges
                .get(&forge.verifying_key())
                .unwrap()
                .get(&log_id)
                .unwrap(),
            &(None, Some(0)),
        );

        // Do it again to show how it was persisted (the "frontier" was reset).
        let ranges = acked.nacked_log_ranges(StreamFrom::Frontier).await.unwrap();
        assert_eq!(
            ranges
                .get(&forge.verifying_key())
                .unwrap()
                .get(&log_id)
                .unwrap(),
            &(None, Some(0)),
        );
    }

    /// M4-22 fix round: a file-backed pool, explicitly without WAL and with `busy_timeout=0`, so a
    /// held-open reader transaction on a second connection deterministically (no timing race)
    /// makes the very next writer commit fail with `database is locked` -- exactly the transient
    /// error class `Acked::ack`'s retry is meant to survive. `Acked::new` accepts any
    /// `SqliteStore`, including one built this way, so this doesn't need `Acked` itself to be
    /// generic over a fault-injecting store type.
    async fn faulty_store() -> (SqliteStore, sqlx::SqlitePool) {
        let path = std::env::temp_dir().join(format!(
            "p2panda-acked-m4-22-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(4)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    use sqlx::Executor;
                    conn.execute("PRAGMA journal_mode=DELETE;").await?;
                    conn.execute("PRAGMA busy_timeout=0;").await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();
        let store = SqliteStore::from_pool(pool.clone());
        p2panda_store::sqlite::run_pending_migrations(&pool)
            .await
            .unwrap();
        (store, pool)
    }

    /// A single transient store error on `ack` must be retried into success, not surfaced --
    /// upstream (`p2panda/src/streams/stream.rs`) maps an `ack` error into `StreamEvent::AckFailed`
    /// instead of `Processed`, even though the operation itself is already durably stored; the
    /// node's consumers never count/height-track an `AckFailed` op (the CI-only
    /// `retention.rs::peer_never_prunes_remote_log` failure signature).
    #[tokio::test]
    async fn ack_retries_single_transient_failure() {
        let topic = Topic::random();
        let (store, pool) = faulty_store().await;
        let credentials = Credentials::generate();
        let forge = OperationForge::new(credentials, store.clone());
        let log_id = LogId::from_topic(topic);
        let acked = Acked::new(store.clone(), topic);

        let operation_0 = forge
            .create_operation(
                Some(topic),
                log_id,
                Some(b"la".to_vec()),
                Extensions::from_topic(topic),
            )
            .await
            .unwrap();

        // Hold a reader transaction open on a second connection from the same pool -- the next
        // writer commit (inside `ack`) is guaranteed to hit `database is locked` immediately
        // (`busy_timeout=0`).
        let mut reader_tx = pool.begin().await.unwrap();
        sqlx::query("SELECT COUNT(*) FROM cursors_v1")
            .fetch_optional(&mut *reader_tx)
            .await
            .unwrap();

        // Release the reader partway through `ack`'s retry backoff budget (10 + 50 + 250 = 310ms
        // worst case) -- comfortably between the 2nd (t=10ms) and 3rd (t=60ms) attempts, so the
        // 3rd attempt succeeds.
        let release = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            reader_tx.rollback().await.unwrap();
        });

        // square-tower fork addition (M4-24): `ack` now only persists every `ACK_BATCH_MAX_OPS`
        // acks or `ACK_BATCH_MAX_DELAY`, whichever comes first (see `AckBatch`) -- force the
        // upcoming `ack` call to be the one that flushes (and therefore actually touches the
        // store, hitting the fault) rather than one of the merely-in-memory ones this test isn't
        // about.
        acked.batch.lock().await.pending = ACK_BATCH_MAX_OPS - 1;

        let result = acked.ack(operation_0).await;
        release.await.unwrap();

        assert!(
            result.is_ok(),
            "a transient lock released mid-retry must recover, got {result:?}"
        );
        assert!(
            acked
                .nacked_log_ranges(StreamFrom::Frontier)
                .await
                .unwrap()
                .is_empty(),
            "the ack must actually be persisted once the retry recovers"
        );
    }

    /// Mutation: calling the un-retried `ack_attempt` directly (bypassing `ack`'s retry loop, i.e.
    /// today's pre-fix behaviour) against the identical held-open-reader fault must surface the
    /// store error -- proving `ack_retries_single_transient_failure` is actually sensitive to the
    /// retry loop's presence.
    #[tokio::test]
    async fn without_retry_ack_attempt_surfaces_the_transient_error() {
        let topic = Topic::random();
        let (store, pool) = faulty_store().await;
        let credentials = Credentials::generate();
        let forge = OperationForge::new(credentials, store.clone());
        let log_id = LogId::from_topic(topic);
        let acked = Acked::new(store.clone(), topic);

        let operation_0 = forge
            .create_operation(
                Some(topic),
                log_id,
                Some(b"la".to_vec()),
                Extensions::from_topic(topic),
            )
            .await
            .unwrap();

        let mut reader_tx = pool.begin().await.unwrap();
        sqlx::query("SELECT COUNT(*) FROM cursors_v1")
            .fetch_optional(&mut *reader_tx)
            .await
            .unwrap();

        // square-tower fork addition (M4-24): force this call to be the one that flushes (see the
        // identical note in `ack_retries_single_transient_failure`).
        acked.batch.lock().await.pending = ACK_BATCH_MAX_OPS - 1;

        let header: &crate::operation::Header = std::borrow::Borrow::borrow(&operation_0);
        let result = acked.ack_attempt(header).await;
        reader_tx.rollback().await.unwrap();

        assert!(
            matches!(result, Err(AckedError::Store(_))),
            "without the retry loop, a single transient failure must surface as a store error \
             (today's pre-fix behaviour, mapped to StreamEvent::AckFailed upstream), got {result:?}"
        );
    }

    /// M4-24: a single `ack` below `ACK_BATCH_MAX_OPS`/`ACK_BATCH_MAX_DELAY` must NOT write the
    /// cursor to the store -- that's the whole point of batching. Reads the store directly
    /// (bypassing `Acked::cursor`, which deliberately serves the fresher in-memory value) to
    /// prove the persisted state actually lagged.
    #[tokio::test]
    async fn single_ack_below_threshold_does_not_persist() {
        use p2panda_store::cursors::CursorStore;

        let topic = Topic::random();
        let store = SqliteStore::temporary().await;
        let credentials = Credentials::generate();
        let forge = OperationForge::new(credentials, store.clone());
        let log_id = LogId::from_topic(topic);
        let acked = Acked::new(store.clone(), topic);

        let operation_0 = forge
            .create_operation(
                Some(topic),
                log_id,
                Some(b"la".to_vec()),
                Extensions::from_topic(topic),
            )
            .await
            .unwrap();

        acked.ack(operation_0).await.unwrap();

        // In-memory state already reflects the ack.
        assert!(
            acked
                .nacked_log_ranges(StreamFrom::Frontier)
                .await
                .unwrap()
                .is_empty(),
            "the in-memory cursor must reflect the ack immediately"
        );

        // The store itself must not have been written to yet: a single ack is far below both
        // `ACK_BATCH_MAX_OPS` and `ACK_BATCH_MAX_DELAY`.
        let persisted = CursorStore::<VerifyingKey, LogId>::get_cursor(&store, acked.cursor_name())
            .await
            .unwrap();
        assert!(
            persisted.is_none(),
            "a single below-threshold ack must not yet be persisted to the store, got {persisted:?}"
        );
    }

    /// M4-24: `flush` must persist a batched-but-not-yet-flushed cursor immediately, regardless
    /// of `ACK_BATCH_MAX_OPS`/`ACK_BATCH_MAX_DELAY` -- required before a graceful shutdown (see
    /// the call site in `stream.rs`'s output-event task).
    #[tokio::test]
    async fn flush_persists_a_pending_batch() {
        use p2panda_store::cursors::CursorStore;

        let topic = Topic::random();
        let store = SqliteStore::temporary().await;
        let credentials = Credentials::generate();
        let forge = OperationForge::new(credentials, store.clone());
        let log_id = LogId::from_topic(topic);
        let acked = Acked::new(store.clone(), topic);

        let operation_0 = forge
            .create_operation(
                Some(topic),
                log_id,
                Some(b"la".to_vec()),
                Extensions::from_topic(topic),
            )
            .await
            .unwrap();

        acked.ack(operation_0).await.unwrap();
        assert!(
            CursorStore::<VerifyingKey, LogId>::get_cursor(&store, acked.cursor_name())
                .await
                .unwrap()
                .is_none(),
            "sanity check: the ack above must not have persisted on its own"
        );

        acked.flush().await.unwrap();

        assert!(
            CursorStore::<VerifyingKey, LogId>::get_cursor(&store, acked.cursor_name())
                .await
                .unwrap()
                .is_some(),
            "flush must persist the pending batch immediately"
        );
    }
}
