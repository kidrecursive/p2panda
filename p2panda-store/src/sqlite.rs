// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite database implementation with associated utility functions.
use std::sync::Arc;
use std::time::Duration;

use p2panda_core::cbor::EncodeError;
use sqlx::migrate::{MigrateDatabase, Migrator};
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Executor, Sqlite, migrate};
use thiserror::Error;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, broadcast};
use tracing::{error, warn};

/// square-tower fork addition (M4-22): journal mode applied to every pool connection on open.
///
/// WAL lets readers (`SqliteStore::execute`, which does not go through the writer-serialising
/// `semaphore` in `Transaction::begin`) proceed concurrently with a writer holding an open
/// transaction, instead of contending for the single rollback-journal file. This is the leading
/// candidate for the `(code: 5) database is locked` errors that silently dropped ingest writes
/// (see `docs/upstream/p2panda-ingest-drop-recovery.md`).
const SQLITE_JOURNAL_MODE_PRAGMA: &str = "PRAGMA journal_mode=wal;";

/// square-tower fork addition (M4-22): busy timeout (milliseconds) applied to every pool
/// connection on open.
///
/// Belt-and-braces alongside WAL: if a connection still finds the database locked (e.g. during
/// a checkpoint), SQLite retries internally for up to this long before returning
/// `SQLITE_BUSY`/`database is locked`, instead of failing immediately.
const SQLITE_BUSY_TIMEOUT_PRAGMA: &str = "PRAGMA busy_timeout=5000;";

/// square-tower fork addition (M4-24): synchronous level applied to every pool connection on
/// open.
///
/// Under WAL, `synchronous=FULL` (SQLite's default) fsyncs the WAL file once per commit, so
/// every `BEGIN IMMEDIATE` write transaction (ingest, orderer, ack) pays a full fsync -- on a
/// slow/fsync-bound disk this caps ingest throughput well below the publish rate of a few
/// drones (D24). `synchronous=NORMAL` under WAL is still crash-safe (a checkpoint always leaves a
/// consistent database; SQLite's own WAL durability guarantee holds), it only relaxes the
/// *ordering* guarantee across a process/OS crash: the most recent commit(s) since the last WAL
/// checkpoint may be rolled back on restart if the machine loses power before that checkpoint's
/// fsync. This is the documented trade-off recorded in D24 -- acceptable here because the
/// example's payload is re-publishable telemetry, not a ledger. Never weaken `BEGIN IMMEDIATE`
/// (that guards the *lock*, this pragma guards the *fsync*; they are orthogonal, see M4-22).
const SQLITE_SYNCHRONOUS_PRAGMA: &str = "PRAGMA synchronous=NORMAL;";

/// Applies the fork's `PRAGMA journal_mode` / `PRAGMA busy_timeout` / `PRAGMA synchronous` to
/// every connection opened by the pool (M4-22, M4-24).
fn with_pragmas(options: SqlitePoolOptions) -> SqlitePoolOptions {
    options.after_connect(|conn, _meta| {
        Box::pin(async move {
            conn.execute(SQLITE_JOURNAL_MODE_PRAGMA).await?;
            conn.execute(SQLITE_BUSY_TIMEOUT_PRAGMA).await?;
            conn.execute(SQLITE_SYNCHRONOUS_PRAGMA).await?;
            Ok(())
        })
    })
}

/// Creates the SQLite database if it doesn't already exist.
pub async fn create_database(url: &str) -> Result<(), SqliteError> {
    if !Sqlite::database_exists(url).await? {
        Sqlite::create_database(url).await?
    }
    Ok(())
}

/// Drops the SQLite database if it exists.
pub async fn drop_database(url: &str) -> Result<(), SqliteError> {
    if Sqlite::database_exists(url).await? {
        Sqlite::drop_database(url).await?
    }
    Ok(())
}

/// square-tower fork addition (M4-22 fix round): reads back `PRAGMA journal_mode` / `PRAGMA
/// busy_timeout` on a connection from the pool and logs them once at store open, so a runtime
/// misconfiguration (e.g. an `:memory:` database silently keeping `journal_mode=memory` --
/// expected and harmless, WAL doesn't apply there -- or, more seriously, a future regression that
/// drops `with_pragmas` from a call site) is visible rather than silently assumed.
async fn log_pragmas_once(pool: &sqlx::SqlitePool) {
    let journal_mode: Result<(String,), _> = sqlx::query_as("PRAGMA journal_mode;")
        .fetch_one(pool)
        .await;
    let busy_timeout: Result<(i64,), _> = sqlx::query_as("PRAGMA busy_timeout;")
        .fetch_one(pool)
        .await;
    // square-tower fork addition (M4-24): also read back `synchronous` so a regression (e.g. a
    // future call site that drops `with_pragmas`) is visible in the same log line rather than
    // silently reverting to SQLite's default `FULL`.
    let synchronous: Result<(i64,), _> = sqlx::query_as("PRAGMA synchronous;")
        .fetch_one(pool)
        .await;
    match (journal_mode, busy_timeout, synchronous) {
        (Ok((journal_mode,)), Ok((busy_timeout,)), Ok((synchronous,))) => {
            tracing::info!(
                journal_mode,
                busy_timeout,
                synchronous,
                "sqlite store opened"
            );
        }
        (journal_mode, busy_timeout, synchronous) => {
            warn!(
                ?journal_mode,
                ?busy_timeout,
                ?synchronous,
                "sqlite store opened, but reading back its own pragmas failed"
            );
        }
    }
}

/// Creates the SQLite connection pool.
pub async fn connection_pool(
    url: &str,
    max_connections: u32,
) -> Result<sqlx::SqlitePool, SqliteError> {
    let pool: sqlx::SqlitePool = with_pragmas(SqlitePoolOptions::new().max_connections(max_connections))
        .connect(url)
        .await?;
    log_pragmas_once(&pool).await;
    Ok(pool)
}

/// Gets migrations from folder without running them.
pub fn migrations() -> Migrator {
    migrate!()
}

/// Runs any pending database migrations from inside the application.
pub async fn run_pending_migrations(pool: &sqlx::SqlitePool) -> Result<(), SqliteError> {
    migrations().run(pool).await?;
    Ok(())
}

/// Builder for `SqliteStore`.
///
/// To create the database call `SqliteStoreBuilder::build()`.
///
/// By default, the builder configures an in-memory database with a maximum number of 16
/// connections. The database is created if it doesn't already exist and migrations are
/// automatically run on start-up.
pub struct SqliteStoreBuilder {
    url: String,
    min_connections: u32,
    max_connections: u32,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
    run_migrations: bool,
    create_database: bool,
}

impl Default for SqliteStoreBuilder {
    fn default() -> Self {
        Self {
            url: ":memory:".into(),
            min_connections: 3,
            max_connections: 16,
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            create_database: true,
            run_migrations: true,
        }
    }
}

impl SqliteStoreBuilder {
    /// Creates a new `SqliteStoreBuilder` using default configuration values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new in-memory `SqliteStoreBuilder` using recommended configuration values.
    ///
    /// The configuration values have been chosen to prevent the in-memory database being dropped
    /// when there are no active connections and the idle timeout or max lifetime limit is reached.
    pub fn memory() -> Self {
        Self::default()
            .database_url(":memory:")
            .min_connections(1)
            .max_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
    }

    /// Sets the database URL.
    ///
    /// If left unset, the database will use an ephemeral in-memory URL.
    pub fn database_url(mut self, url: &str) -> Self {
        self.url = url.to_string();
        self
    }

    /// Sets the minimum number of connections to be maintained by the database pool.
    ///
    /// If left unset, a minimum of 3 connections will be maintained.
    pub fn min_connections(mut self, min_connections: u32) -> Self {
        self.min_connections = min_connections;
        self
    }

    /// Sets the maximum number of connections to be maintained by the database pool.
    ///
    /// If left unset, a maximum of 16 connections will be maintained.
    pub fn max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Set a maximum idle duration for individual connections.
    ///
    /// Any connection that remains in the idle queue longer than this will be closed.
    pub fn idle_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.idle_timeout = timeout.into();
        self
    }

    /// Set the maximum lifetime of individual connections.
    ///
    /// Any connection with a lifetime greater than this will be closed.
    ///
    /// When set to `None`, all connections live until either reaped by `idle_timeout` or explicitly
    /// disconnected.
    ///
    /// Infinite connections are not recommended due to the unfortunate reality of memory/resource
    /// leaks on the database-side. It is better to retire connections periodically (even if only
    /// once daily) to allow the database the opportunity to clean up data structures (parse trees,
    /// query metadata caches, thread-local storage, etc.) that are associated with a session.
    pub fn max_lifetime(mut self, lifetime: impl Into<Option<Duration>>) -> Self {
        self.max_lifetime = lifetime.into();
        self
    }

    /// Creates the database if it doesn't already exist.
    ///
    /// If left unset, the database will be created by default.
    pub fn create_database(mut self, create_database: bool) -> Self {
        self.create_database = create_database;
        self
    }

    /// Sets whether pending migrations should be applied when the database is built.
    ///
    /// If left unset, the database will apply any pending migrations.
    pub fn run_default_migrations(mut self, run_migrations: bool) -> Self {
        self.run_migrations = run_migrations;
        self
    }

    /// Builds the `SqliteStore`.
    pub async fn build(self) -> Result<SqliteStore, SqliteError> {
        if self.create_database {
            create_database(&self.url).await?;
        }

        let pool: sqlx::SqlitePool = with_pragmas(
            SqlitePoolOptions::new()
                .min_connections(self.min_connections)
                .max_connections(self.max_connections)
                .idle_timeout(self.idle_timeout)
                .max_lifetime(self.max_lifetime),
        )
        .connect(&self.url)
        .await?;

        if self.run_migrations {
            run_pending_migrations(&pool).await?;
        }

        log_pragmas_once(&pool).await;

        Ok(SqliteStore::new(pool))
    }
}

/// An in-progress database transaction.
pub type Transaction<'a> = sqlx::Transaction<'a, Sqlite>;

/// Sqlite connection pool.
pub type SqlitePool = sqlx::SqlitePool;

/// SQLite database with connection pool and transaction provider.
///
/// This struct can be cloned and used in multiple places in the application. Every cloned instance
/// will re-use the same connection pool and have access to the same transaction instance if one
/// was started. To guard against sharing transactions unknowingly across unrelated database
/// queries, a concept of a `TransactionPermit` was introduced which does not protect from misuse
/// but helps to make "holding" a transaction explicit.
///
/// Please note that SQLite strictly serializes transactions with _writes_ and will block any
/// parallel attempt to begin another one. Processes starting a transaction will acquire a
/// `TransactionPermit` and keep it until the transaction was committed or rolled back. If the
/// query only involves _reads_ it is recommended to not use transactions and use the `execute`
/// method directly as acquiring transactions will potentially block other processes to do work.
///
/// ## Design decisions
///
/// This storage API design was chosen to make the dynamics of the underlying SQLite database
/// explicit to avoid potentially introducing subtle bugs. Internally any process can access the
/// transaction object to do writes and (uncommitted) reads (see "Transaction I" in diagram). Care
/// is required when designing systems like that as it's still possible to allow concurrent
/// processes to read and write within the same transaction (for example one process could roll
/// back the transaction while the other one assumed it will be committed). Usually developers want
/// to design _writes_ to the database within a transaction if they need consistency and atomicity
/// guarantees. "Unrelated" queries _can_ be "pooled" in one transaction (for performance reasons
/// for example) if consistency is guaranteed by all involved processes and the underlying
/// data-model (see "Transaction II" in diagram).
///
/// ```text
/// Transaction I:
/// begin ---------------------> commit
///
/// Process I:
///       --> write --> read -->
///
///                                             Transaction II:
///                                             begin ----------------------> commit
///
///                                             Process II:
///                                                   --> write --> write -->
///
///                                             Process III:
///                                                   --> read --> write --->
/// ```
///
/// Another design decision is to not expose transactions to the high-level storage APIs (similar
/// to the "Repository Pattern"). Users of the storage methods like `get_operation` (in
/// `OperationStore`) etc. do _not_ need to explicity deal with transaction objects, as this is
/// handled internally now. Like this it is possible to separate the "logic" from the "storage"
/// layer and keep the code clean.
#[derive(Clone, Debug)]
pub struct SqliteStore {
    tx: Arc<Mutex<Option<Transaction<'static>>>>,
    pub(crate) pool: sqlx::SqlitePool,
    semaphore: Arc<Semaphore>,
    /// square-tower fork addition (D3-u, M4-21): notifies of every *new* topic/author/data_id
    /// association (`TopicStore::associate`'s `is_new` case), encoded-topic-bytes as the payload,
    /// so `subscribe_new_associations` can filter to a single topic. This is the sole real
    /// association choke point in the store -- used by `p2panda-net`'s topic manager to detect
    /// structural drift for event-driven resync, without waiting for the periodic resync timer.
    pub(crate) assoc_tx: broadcast::Sender<Vec<u8>>,
}

impl SqliteStore {
    /// Creates a new `SqliteStore` using the provided connection pool.
    pub(crate) fn new(pool: sqlx::SqlitePool) -> Self {
        let (assoc_tx, _) = broadcast::channel(64);
        Self {
            tx: Arc::default(),
            pool,
            // SQLite only ever allows _one_ transaction at a time. This might be a repetition of
            // what sqlx and SQLite do under the hood, but we want to make this behaviour explicit
            // right from the beginning with this semaphore.
            semaphore: Arc::new(Semaphore::new(1)),
            assoc_tx,
        }
    }

    /// Creates a new `SqliteStore` using the provided connection pool.
    pub fn from_pool(pool: sqlx::SqlitePool) -> Self {
        Self::new(pool)
    }

    /// Returns a reference to the connection pool.
    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }

    /// Builds an in-memory SQLite database for testing purposes.
    #[cfg(any(test, feature = "test_utils"))]
    pub async fn temporary() -> Self {
        SqliteStoreBuilder::memory()
            .build()
            .await
            .expect("migrations succeeded")
    }

    /// Executes a SQL query within a transaction.
    ///
    /// This method will return an error when no transaction is currently given. Make sure to call
    /// `begin` before.
    ///
    /// If the query fails the user probably wants to roll back the transaction and free the
    /// permit. This is _not_ handled automatically.
    pub async fn tx<F, R>(&self, f: F) -> Result<R, SqliteError>
    where
        F: AsyncFnOnce(&mut Transaction) -> Result<R, SqliteError>,
    {
        let mut tx_ref = self.tx.lock().await;
        let tx = tx_ref.as_mut().ok_or(SqliteError::TransactionMissing)?;

        f(tx).await
    }

    /// Executes a SQL query directly against the pool, without acquiring the write-transaction
    /// permit.
    ///
    /// square-tower fork addition (M4-24): this is the required path for read-only queries.
    /// `begin`/`tx` serialise every caller behind a single semaphore permit and a `BEGIN
    /// IMMEDIATE` write lock (M4-22) -- correct for writes, but a pure read taking that same
    /// permit needlessly blocks every other write in the process behind it, capping throughput
    /// on a slow/fsync-bound disk. Under WAL, `execute` (an ordinary pooled connection, no
    /// transaction) reads the last-committed snapshot concurrently with a writer holding an open
    /// `BEGIN IMMEDIATE` transaction, so it never contends for the write lock. Every read-only
    /// method on this store (`resolve`, `get_latest_entry`, `get_log_heights`, `get_log_size`,
    /// cursor reads, etc.) already goes through this method rather than `begin`/`tx` (audited
    /// M4-24: the store's write-transaction call sites -- `forge.rs`, `node.rs`, the address
    /// book actor, `acked.rs`'s cursor writes, `spaces/member.rs`, `spaces/space.rs` -- all
    /// perform at least one write inside the transaction; none wrap a read-only body). Only use
    /// `begin`/`tx` when the query needs to write, or needs read-then-write atomicity with a
    /// write later in the same transaction (e.g. `get_latest_entry_tx` before `insert_operation`).
    pub async fn execute<F, R>(&self, f: F) -> Result<R, SqliteError>
    where
        F: AsyncFnOnce(&sqlx::SqlitePool) -> Result<R, SqliteError>,
    {
        f(&self.pool).await
    }
}

impl crate::traits::Transaction for SqliteStore {
    type Error = SqliteError;

    type Permit = TransactionPermit;

    /// Begins a transaction.
    ///
    /// Transactions are strictly serialized, this is expressed in form of a `TransactionPermit`
    /// processes need to hold when acquiring access to a new transaction. Any concurrent process
    /// calling it will await here if there's already another process holding a permit, this will
    /// potentially "slow down" work and should be carefully used.
    ///
    /// Any process with a transaction can now start using the `tx` method to execute writes within
    /// this transaction or perform uncommitted "dirty" reads on it.
    ///
    /// It is usually not necessary to acquire a transaction when the logic only requires committed
    /// _reads_ to the database. Use `execute` instead.
    ///
    /// square-tower fork addition (M4-24): this is a hard rule, not just a recommendation --
    /// `begin` serialises every caller in the process behind one semaphore permit plus a `BEGIN
    /// IMMEDIATE` write-lock acquisition (M4-22), so a read-only caller taking it blocks every
    /// concurrent writer (ingest, orderer, ack) for no reason. Reserve `begin`/`tx` for queries
    /// that write, or that must read-then-write atomically within the same transaction.
    async fn begin(&self) -> Result<TransactionPermit, SqliteError> {
        // Acquire a permit from the semaphore, it will await if currently another process has the
        // permit. Here we enforce strict serialization of transactions (similar to what SQLite
        // does under the hood).
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("if semaphore is closed then the whole struct is gone as well");

        // Access the transaction object which we've placed behind a Mutex. This lock follows a
        // different logic and only makes sure that mutable access to it is exclusive _within_ a
        // process "holding" the transaction permit.
        let mut tx_ref = self.tx.lock().await;

        // Under normal operation this is always `None` here: `TransactionPermit::drop` rolls back
        // and clears any transaction left behind by an early return / error, and only releases the
        // semaphore permit (what gates a NEW `begin` reaching this point) once that rollback has
        // completed. A leftover `Some` here means a task holding the permit was cancelled (e.g.
        // `tokio::task::JoinHandle::abort` during shutdown) before its `TransactionPermit`'s Drop
        // impl's own cleanup task ever got polled to completion under scheduler/CPU pressure --
        // M4-12 (`square-tower/M4-12-store-teardown-assert.md`, D3-q). Rather than assert/panic on
        // a detached task in that case (poisoning the store for every future caller with no way to
        // recover), roll the stale transaction back here, on the new caller's own task, before
        // starting the new one: no half-applied operation from the aborted holder ever survives,
        // and `begin` always returns `Ok` once the permit is held.
        if let Some(stale_tx) = tx_ref.take() {
            warn!(
                "SqliteStore::begin: rolling back a transaction left behind by a cancelled \
                 holder (M4-12); this indicates a task was aborted while holding a store \
                 transaction"
            );
            // If the rollback itself fails (typically: the aborted holder's connection is already
            // broken), SQLite discards the uncommitted transaction together with that connection,
            // so no write from the cancelled holder can become visible either way -- but say so
            // loudly instead of swallowing the error (trust-boundary review of M4-12): a failing
            // rollback on a healthy connection would be a real store defect worth investigating.
            if let Err(err) = stale_tx.rollback().await {
                error!(
                    %err,
                    "SqliteStore::begin: rolling back the stale transaction failed; proceeding \
                     with a fresh transaction (the stale one is discarded with its connection)"
                );
            }
        }

        // square-tower fork addition (M4-22 fix round): `BEGIN IMMEDIATE` instead of SQLite's
        // default deferred `BEGIN`. A deferred transaction that reads first (as most of this
        // store's write transactions do, e.g. `get_latest_entry_tx` before `insert_operation`)
        // establishes its snapshot at that first read; under WAL, if any other connection commits
        // a change before this transaction's own later write, the write fails immediately with
        // `SQLITE_BUSY_SNAPSHOT` -- `PRAGMA busy_timeout` (`with_pragmas`, above) never helps this
        // specific error, since it isn't lock contention this connection can usefully wait out
        // (the fix is a fresh snapshot, not a longer wait). `BEGIN IMMEDIATE` acquires the write
        // lock at the very start instead, so a transaction that would otherwise race a concurrent
        // committer instead queues on the lock and waits (up to `busy_timeout`) like an ordinary
        // writer-vs-writer conflict. Confirmed empirically (fleet chaos run on the pragma-only fix,
        // `9d107819`): 19 distinct `database is locked` errors on control, still present despite
        // WAL + busy_timeout=5000.
        let tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        tx_ref.replace(tx);

        Ok(TransactionPermit::new(permit, self.tx.clone()))
    }

    /// Rolls back the transaction and with that all uncommitted changes.
    ///
    /// This takes the permit and frees it after the rollback has finished. Other processes can now
    /// begin new transactions.
    async fn rollback(&self, permit: TransactionPermit) -> Result<(), SqliteError> {
        let Some(tx) = self.tx.lock().await.take() else {
            panic!("can't have no transaction without dropping permit first")
        };

        let result = tx.rollback().await.map_err(SqliteError::Sqlite);

        // Always drop the permit, both on successful rollback and error. This will allow other
        // processes now to begin a new transaction and acquire the permit.
        permit.mark_committed_and_drop();

        result
    }

    /// Commits the transaction.
    ///
    /// This takes the permit and frees it after the commit has finished. Other processes can now
    /// begin new transactions.
    async fn commit(&self, permit: TransactionPermit) -> Result<(), SqliteError> {
        let Some(tx) = self.tx.lock().await.take() else {
            panic!("can't have no transaction without dropping permit first")
        };

        let result = tx.commit().await.map_err(SqliteError::Sqlite);

        // Always drop the permit, both on successful commit and error. This will allow other
        // processes now to begin a new transaction and acquire the permit.
        permit.mark_committed_and_drop();

        result
    }
}

/// Locked context marking the lifetime of a single transaction.
pub struct TransactionPermit {
    permit: Arc<OwnedSemaphorePermit>,
    tx: Arc<Mutex<Option<Transaction<'static>>>>,
    committed: bool,
}

impl TransactionPermit {
    /// Creates a new `TransactionPermit` using the given permit and transaction.
    pub(super) fn new(
        permit: OwnedSemaphorePermit,
        tx: Arc<Mutex<Option<Transaction<'static>>>>,
    ) -> Self {
        Self {
            permit: Arc::new(permit),
            tx,
            committed: false,
        }
    }

    /// Marks the transaction as committed and drops the permit.
    ///
    /// In the case that the permit was never used, whether due to an early return or error, the
    /// transaction is automatically rolled-back to prevent corrupted state.
    pub(super) fn mark_committed_and_drop(mut self) {
        self.committed = true;
        drop(self)
    }
}

impl Drop for TransactionPermit {
    fn drop(&mut self) {
        // If the permit was never used (due to an early return / error / etc.) we automatically
        // roll-back the transaction.
        if !self.committed {
            let permit = self.permit.clone();
            let tx = self.tx.clone();

            tokio::spawn(async move {
                if let Some(tx) = tx.lock().await.take() {
                    let _ = tx.rollback().await;
                }

                drop(permit); // Semaphore released only after rollback completes.
            });
        }
    }
}

/// Error when interacting with a SQLite store implementation.
#[derive(Debug, Error)]
pub enum SqliteError {
    /// This is a critical error as it indicates that something is wrong with the usage of this
    /// API: Queries using transactions can only ever occur if a transaction was started _before_.
    #[error("tried to interact with inexistant transaction")]
    TransactionMissing,

    /// SQLite database and connection error.
    #[error(transparent)]
    Sqlite(#[from] sqlx::Error),

    /// SQL table schema migration error.
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),

    /// An I/O error occurred while encoding bytes before storing them into the database. This is a
    /// critical error.
    #[error("failed encoding '{0}' value before storing to database: {1}")]
    Encode(String, EncodeError),

    /// Invalid, corrupted data was found in the database. This is a critical error.
    #[error("could not decode corrupted '{0}' value from database: {1}")]
    Decode(String, DecodeError),
}

/// Error decoding value retrieved from a store.
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error(transparent)]
    Cbor(#[from] p2panda_core::cbor::DecodeError),

    #[error(transparent)]
    Header(#[from] p2panda_core::operation::HeaderError),

    #[error(transparent)]
    Hash(#[from] p2panda_core::hash::HashError),

    #[error(transparent)]
    Topic(#[from] p2panda_core::topic::TopicError),

    #[error("parsing from string failed")]
    FromStr,
}

#[cfg(test)]
mod tests {
    use futures_test::task::noop_context;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Executor, query, query_as, query_scalar};
    use tokio::pin;

    use crate::sqlite::{SqliteError, SqliteStore, connection_pool};
    use crate::traits::Transaction;

    #[tokio::test]
    async fn transaction_provider() {
        let pool = SqliteStore::temporary().await;

        // Executing with an in-existant transaction should throw error.
        std::assert_matches!(
            pool.tx(async |_| Ok(())).await,
            Err(SqliteError::TransactionMissing)
        );

        // Starting a new transaction should work.
        let permit = pool.begin().await.expect("no error");

        // .. attempting to start a second one should make us wait.
        {
            let fut = pool.begin();
            let mut cx = noop_context();
            pin!(fut);
            assert!(fut.poll(&mut cx).is_pending());
        }

        // Using the transaction should work without failure.
        assert!(pool.tx(async |_| Ok(())).await.is_ok());

        // Committing should work as well.
        assert!(pool.commit(permit).await.is_ok());

        // .. and now running a transaction should fail.
        std::assert_matches!(
            pool.tx(async |_| Ok(())).await,
            Err(SqliteError::TransactionMissing)
        );
    }

    #[tokio::test]
    async fn early_permit_drop_causing_rollback() {
        let pool = SqliteStore::temporary().await;

        // Create test-table schema.
        pool.execute(async |pool| {
            pool.execute("CREATE TABLE test(x INTEGER)").await?;
            Ok(())
        })
        .await
        .unwrap();

        let permit = pool.begin().await.unwrap();

        pool.tx(async |tx| {
            query("INSERT INTO test (x) VALUES (10)")
                .execute(&mut **tx)
                .await?;
            Ok(())
        })
        .await
        .unwrap();

        // Permit was dropped prematurely without committing.
        drop(permit);

        // It is okay to start another permit.
        assert!(pool.begin().await.is_ok());

        // The data was not written as the transaction got rolled back.
        let count: i64 = pool
            .execute(async |pool| {
                query_scalar("SELECT COUNT(*) FROM test")
                    .fetch_one(pool)
                    .await
                    .map_err(SqliteError::Sqlite)
            })
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    /// M4-12 (`square-tower/M4-12-store-teardown-assert.md`, D3-q): reproduces a task cancelled
    /// while holding a `begin()`ed transaction -- `Handle::shutdown`'s
    /// `tokio::task::JoinHandle::abort` in the downstream square-tower node -- without depending
    /// on real scheduler/CPU-pressure timing (upstream only saw this 1/10 stressed local runs).
    ///
    /// `TransactionPermit::drop`'s cleanup path (`tokio::spawn`s a task that takes `tx_ref` and
    /// rolls it back, then drops the semaphore permit) needs an entered runtime to schedule that
    /// task onto. If the runtime is torn down before the spawned task is ever polled -- exactly
    /// what a process shutdown shortly after an abort can do -- the task's captured
    /// `Arc<OwnedSemaphorePermit>` still gets dropped along with the rest of the never-polled
    /// future (releasing the semaphore), but the code path that clears `tx_ref`
    /// (`tx.lock().await.take()`) never runs, since the future's body was never executed at all.
    /// This test reproduces that exact end state directly -- semaphore released, `tx_ref` still
    /// `Some` -- via the same private fields `begin`/`TransactionPermit::drop` use, rather than
    /// via a real task abort (whose timing this test would otherwise inherit).
    #[tokio::test]
    async fn cancelled_holder_never_poisons_begin() {
        let store = SqliteStore::temporary().await;

        store
            .execute(async |pool| {
                pool.execute("CREATE TABLE test(x INTEGER)").await?;
                Ok(())
            })
            .await
            .unwrap();

        // Mirrors `begin()`'s own steps (acquire the semaphore permit, open a transaction, place
        // it in `tx_ref`), then the write `tx()` would run through it.
        let sem_permit = store.semaphore.clone().acquire_owned().await.unwrap();
        let mut raw_tx = store.pool.begin().await.unwrap();
        query("INSERT INTO test (x) VALUES (99)")
            .execute(&mut *raw_tx)
            .await
            .unwrap();
        store.tx.lock().await.replace(raw_tx);

        // Mirrors what `TransactionPermit::drop`'s cleanup task's captured permit clone does when
        // dropped without ever being polled: release the semaphore, `tx_ref` untouched.
        drop(sem_permit);

        // Old code: this `begin()` panics ("can't have an already existing transaction after an
        // just-acquired permit") -- the semaphore permit was released above, but `tx_ref` was
        // never cleared. Fixed code: rolls the stale transaction back (with a `warn!`) and
        // returns `Ok`.
        let permit_2 = store
            .begin()
            .await
            .expect("begin should not panic after a cancelled holder (M4-12)");
        // Frees the sole (`max_connections(1)`, in-memory) connection back to the pool so the
        // count check below (via `execute`, a separate acquisition) doesn't itself time out.
        store.rollback(permit_2).await.unwrap();

        // The cancelled holder's write must not survive -- rolled back, not half-applied.
        let count: i64 = store
            .execute(async |pool| {
                query_scalar("SELECT COUNT(*) FROM test")
                    .fetch_one(pool)
                    .await
                    .map_err(SqliteError::Sqlite)
            })
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "a cancelled holder's write must not survive (M4-12)"
        );
    }

    #[tokio::test]
    async fn serialized_transactions() {
        let pool_1 = SqliteStore::temporary().await;

        let pool_2 = pool_1.clone();

        // Create test-table schema.
        pool_1
            .execute(async |pool| {
                pool.execute("CREATE TABLE test(x INTEGER)").await?;
                Ok(())
            })
            .await
            .unwrap();

        // 1. Pool 1 acquires the permit to run a transaction.
        let permit_1 = pool_1.begin().await.unwrap();

        // .. parallely Pool 2 also tries to do some work.
        let handle = tokio::spawn(async move {
            // Try to acquire a permit, this will "block" for now as pool 1 already is doing
            // something and we need to wait.
            let permit_2 = pool_2.begin().await.unwrap();

            // 5. We should see now the previously change made by pool 1.
            let result = pool_2
                .tx(async |tx| {
                    let row: (i64,) = query_as("SELECT x FROM test").fetch_one(&mut **tx).await?;
                    Ok(row.0)
                })
                .await
                .unwrap();
            assert_eq!(result, 5);

            // 6. Change the value to something else.
            pool_2
                .tx(async |tx| {
                    query("INSERT INTO test (x) VALUES (10)")
                        .execute(&mut **tx)
                        .await?;
                    Ok(())
                })
                .await
                .unwrap();

            // 7. .. but abort the transaction and roll back.
            pool_2.rollback(permit_2).await.unwrap();

            // The value should still be the same as before.
            let result = pool_2
                .execute(async |pool| {
                    let row: (i64,) = query_as("SELECT x FROM test").fetch_one(pool).await?;
                    Ok(row.0)
                })
                .await
                .unwrap();
            assert_eq!(result, 5);
        });

        // 2. Pool 1 changes the value.
        pool_1
            .tx(async |tx| {
                query("INSERT INTO test (x) VALUES (5)")
                    .execute(&mut **tx)
                    .await?;
                Ok(())
            })
            .await
            .unwrap();

        // 3. Result is already 5 during "dirty read".
        let result = pool_1
            .tx(async |tx| {
                let row: (i64,) = query_as("SELECT x FROM test").fetch_one(&mut **tx).await?;
                Ok(row.0)
            })
            .await
            .unwrap();
        assert_eq!(result, 5);

        // 4. Commit the change to database and free permit. This will allow now pool_2 to read the
        //    changed value.
        pool_1.commit(permit_1).await.unwrap();

        // Result is still 5 after commit.
        let result = pool_1
            .execute(async |pool| {
                let row: (i64,) = query_as("SELECT x FROM test").fetch_one(pool).await?;
                Ok(row.0)
            })
            .await
            .unwrap();
        assert_eq!(result, 5);

        // Make sure we give pool 2 the time it needs to finish.
        handle.await.unwrap();
    }

    /// Builds a file-backed pool so multiple real connections can contend for the same database
    /// (unlike `SqliteStore::temporary()`, which is a single-connection `:memory:` database and
    /// can never observe cross-connection locking).
    fn temp_db_url(label: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "p2panda-store-m4-22-{label}-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        format!("sqlite://{}?mode=rwc", path.display())
    }

    fn is_locked_error(err: &sqlx::Error) -> bool {
        let msg = err.to_string();
        msg.contains("database is locked") || msg.contains("SQLITE_BUSY")
    }

    /// Opens a second, independent connection straight from the pool (modelling a concurrent
    /// process/connection, the way `p2panda-net`'s status queries and a second node's writes
    /// really are independent connections) and starts -- but never releases -- a read
    /// transaction on it, then runs `writes` concurrent single-row insert-and-commit cycles
    /// through the given `SqliteStore`'s own API (begin/tx/commit, `SqliteStore::execute`'s
    /// sibling write path) while that reader transaction stays open throughout. Returns the
    /// number of writes (of `writes`) that surfaced a "database is locked" / `SQLITE_BUSY` error.
    ///
    /// Under a rollback-journal (`DELETE`) database, a writer's `COMMIT` can be blocked by any
    /// connection still holding an open read transaction (needs to briefly upgrade to an
    /// exclusive lock); under WAL, readers and writers never block each other, so every write
    /// here must still succeed with the reader open the whole time.
    async fn writes_under_open_reader(
        pool: &sqlx::SqlitePool,
        store: &SqliteStore,
        writes: usize,
    ) -> usize {
        let mut reader_tx = pool.begin().await.expect("reader transaction begins");
        let _held: i64 = query_scalar("SELECT COUNT(*) FROM test")
            .fetch_one(&mut *reader_tx)
            .await
            .expect("reader transaction reads");

        let mut lock_errors = 0;
        for i in 0..writes {
            let permit = store.begin().await.expect("writer begins");
            let insert = store
                .tx(async |tx| {
                    query("INSERT INTO test (x) VALUES (?)")
                        .bind(i as i64)
                        .execute(&mut **tx)
                        .await
                        .map_err(SqliteError::Sqlite)
                })
                .await;
            let commit = match insert {
                Ok(_) => store.commit(permit).await,
                Err(_) => {
                    store.rollback(permit).await.expect("rollback succeeds");
                    insert.map(|_| ())
                }
            };
            if let Err(SqliteError::Sqlite(err)) = &commit
                && is_locked_error(err)
            {
                lock_errors += 1;
                // Deterministic once the reader is held open: every subsequent write will hit
                // the exact same lock, so stop early instead of paying out `writes` more
                // `busy_timeout` waits for no new information.
                break;
            }
        }

        // Release the held-open reader last, so its lifetime spans the whole loop above.
        reader_tx.rollback().await.expect("reader transaction ends");
        lock_errors
    }

    /// M4-22: with the fork's `PRAGMA journal_mode=wal` / `PRAGMA busy_timeout` applied to every
    /// pool connection (`with_pragmas`, used by both `connection_pool` and
    /// `SqliteStoreBuilder::build`), a writer must be able to commit while a second connection
    /// holds an open read transaction on the same database, without surfacing
    /// `database is locked` / `SQLITE_BUSY` -- this was silently dropping ingest writes (see
    /// `docs/upstream/p2panda-ingest-drop-recovery.md`).
    ///
    /// Exercises `connection_pool` rather than `SqliteStoreBuilder::build` deliberately: `build`
    /// creates the database through sqlx's own `Sqlite::create_database`, which (as of sqlx
    /// 0.9.0) *already* defaults new databases to WAL via its internal, explicitly
    /// "UNSTABLE: for use by sqlx-cli only" `CREATE_DB_WAL` flag -- so a `build`-based test can't
    /// tell our explicit pragma apart from that undocumented upstream default. `connection_pool`
    /// never goes through `create_database`/migrations at all, so it has no such accidental WAL
    /// mode and genuinely depends on `with_pragmas`; this is exactly the gap this fix closes
    /// (`connection_pool` is public API, used whenever a caller wants a pool without the
    /// `SqliteStoreBuilder` machinery) and it doubles as the harness that can prove the fix does
    /// something even though `SqliteStoreBuilder::build`'s own default WAL-ness is currently a
    /// happy accident of an unstable upstream internal, not a guarantee this fork should rely on.
    ///
    /// Mutation-proof: reverting the fix (removing the `with_pragmas` call in `connection_pool`,
    /// i.e. falling back to a plain rollback-journal database) makes the "fixed" pool behave
    /// exactly like the `control_pool` built explicitly without WAL below, which is asserted to
    /// hit at least one lock error over the same 200 writes -- proving the harness itself can
    /// fail before trusting its "fixed" assertion.
    #[tokio::test]
    async fn concurrent_access_does_not_lock_with_pragmas() {
        const WRITES: usize = 200;

        // Control: explicitly no WAL (matches the on-disk journal mode of the code before this
        // fix) -- must observe at least one lock error, proving this harness can fail.
        let control_pool = SqlitePoolOptions::new()
            .max_connections(4)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    conn.execute("PRAGMA journal_mode=DELETE;").await?;
                    // Fails fast instead of waiting out sqlx's own 5s default busy_timeout on
                    // every one of the 200 writes below -- the reader never releases its lock
                    // regardless, so the outcome is the same either way, just much slower.
                    conn.execute("PRAGMA busy_timeout=0;").await?;
                    Ok(())
                })
            })
            .connect(&temp_db_url("control"))
            .await
            .unwrap();
        let control_store = SqliteStore::from_pool(control_pool.clone());
        control_store
            .execute(async |pool| {
                pool.execute("CREATE TABLE test(x INTEGER)").await?;
                Ok(())
            })
            .await
            .unwrap();
        let control_lock_errors =
            writes_under_open_reader(&control_pool, &control_store, WRITES).await;
        assert!(
            control_lock_errors > 0,
            "control (rollback-journal, no WAL) is expected to observe at least one lock error \
             while a reader transaction stays open across {WRITES} writer commits -- if it \
             never does, this harness can't prove the fix does anything"
        );

        // Fixed: goes through the real `connection_pool` helper, i.e. production code path.
        let fixed_pool = connection_pool(&temp_db_url("fixed"), 4).await.unwrap();
        let fixed_store = SqliteStore::from_pool(fixed_pool.clone());
        fixed_store
            .execute(async |pool| {
                pool.execute("CREATE TABLE test(x INTEGER)").await?;
                Ok(())
            })
            .await
            .unwrap();
        let fixed_lock_errors =
            writes_under_open_reader(&fixed_pool, &fixed_store, WRITES).await;
        assert_eq!(
            fixed_lock_errors, 0,
            "PRAGMA journal_mode=wal must let writes commit while a reader transaction stays \
             open (control observed {control_lock_errors} lock errors over the same {WRITES} \
             writes)"
        );
    }

    /// M4-22 fix round: `SqliteStore::begin` must use `BEGIN IMMEDIATE`, not SQLite's default
    /// deferred `BEGIN`. A deferred transaction that reads first (as most of this store's write
    /// transactions do, e.g. ingest's `get_latest_entry_tx` before `insert_operation`) establishes
    /// its snapshot at that read and takes no write lock at all until its own first write
    /// statement; under WAL, if another connection commits a write in that window, this
    /// transaction's own later write fails immediately with `SQLITE_BUSY_SNAPSHOT` --
    /// `busy_timeout` never helps (it isn't lock contention to wait out; the fix needs a fresh
    /// snapshot, not a longer wait). `BEGIN IMMEDIATE` acquires the write lock at `begin()` itself,
    /// before any statement runs, closing that window entirely.
    ///
    /// Deterministic (no timing race, and no risk of the deadlock a "make B commit before A"
    /// design would have under a correct fix -- B *can't* commit before A once A holds the lock
    /// from `begin()`): open a transaction via the real `SqliteStore::begin` and, *before running
    /// any statement on it*, have a second, independent connection (raw, straight from the pool)
    /// attempt a write with `busy_timeout=0` -- so it fails immediately rather than waiting, if
    /// blocked. Under `BEGIN IMMEDIATE`, that probe must find the table already locked (`begin()`
    /// itself took the write lock). Under a plain deferred `BEGIN` (mutation below), `begin()`
    /// alone takes no lock at all, so the same probe must succeed.
    #[tokio::test]
    async fn begin_takes_the_write_lock_immediately() {
        let url = temp_db_url("begin-immediate");
        let pool = connection_pool(&url, 4).await.unwrap();
        let store = SqliteStore::from_pool(pool.clone());
        store
            .execute(async |pool| {
                pool.execute("CREATE TABLE test(x INTEGER)").await?;
                pool.execute("INSERT INTO test (x) VALUES (0)").await?;
                Ok(())
            })
            .await
            .unwrap();

        // A second, independent connection with `busy_timeout=0` -- fails fast instead of
        // waiting, so this test doesn't need to guess a timeout long enough for CI.
        let probe_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    conn.execute("PRAGMA busy_timeout=0;").await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();

        // `begin()` alone -- no statement run on this transaction yet.
        let permit = store.begin().await.unwrap();

        let probe_result = sqlx::query("UPDATE test SET x = 99")
            .execute(&probe_pool)
            .await;

        store.rollback(permit).await.unwrap();

        assert!(
            is_locked_error(&probe_result.unwrap_err()),
            "BEGIN IMMEDIATE must take the write lock at begin() itself, before any statement, \
             so a concurrent writer's probe (busy_timeout=0) must fail immediately"
        );
    }
}
