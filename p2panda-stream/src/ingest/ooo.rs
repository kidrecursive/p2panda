// SPDX-License-Identifier: MIT OR Apache-2.0

use std::hash::Hash as StdHash;
use std::sync::Arc;

use indexmap::IndexMap;
use p2panda_core::{AnyHeader, Extensions, Hash, LogId, Operation, VerifyingKey};
use tokio::sync::Mutex;
use tracing::warn;

#[derive(Debug)]
pub enum OooResult<'a, E> {
    /// Operation is already in-order and doesn't need buffering.
    InOrder(&'a Operation<E>),

    /// Operation freed buffered items which are now in-order.
    ///
    /// The incoming operation itself is also included in the array.
    Ordered(Vec<Operation<E>>),

    /// Operation is out-of-order and will be buffered.
    OutOfOrder,

    /// Operation is from before a pruning point and thus outdated.
    Outdated,
}

/// Out-of-order (ooo) buffer allowing a configurable window for handling operations with no
/// predecessors yet.
///
/// For every operation this buffer checks if it arrived out-of-order. If yes, it is stored in the
/// internal ring-buffer. If the buffer runs full the oldest item gets evicted first.
///
/// ## Example
///
/// If a log is at log height `[1]` (frontier) and an operation of sequence number `[3]` arrives, it
/// can not be appended to the log due to the strict nature of an append-only log. It will be pushed
/// into the buffer, awaiting the missing `[2]` operation:
///
/// ```text
///          [0] <- [1] <- Log in database
///
/// [3] <- Incoming operation
///
/// => Push into ooo-Buffer.
/// ```
///
/// Operation `[2]` arrives which will "free" the out-of-order items in the buffer, making them "in
/// order". It will release them from the buffer and forward to the user for further validation and
/// finally insertion into the database:
///
/// ```text
///          [0] <- [1] <- Log in database
///
///          [3] <- Operation in ooo-Buffer
///
/// [2] <- Incoming operation
///
/// => Return [2, 3]
/// ```
///
/// ## Assumptions
///
/// Please make sure to only process items which have been checked before against:
///
/// 1. Incoming operations from tombstoned logs / topics were filtered out before.
/// 2. Duplicate, already ingested operations have been filtered out before.
///
/// ## Security note (M4-05 review fix)
///
/// Buffer entries are keyed by `(author, backlink, log_id)`, not just `(backlink, log_id)`: a
/// `LogId` is frequently shared by every author on a topic (e.g. the constant member/key-bundle
/// log id), so keying on `(backlink, log_id)` alone would let one authorized author forge an
/// operation whose `header.backlink` equals the hash of a *different* author's real operation --
/// colliding with that author's own chain lookup (or overwriting their already-buffered entry at
/// the same key) and letting a forged entry ride along in that author's `Ordered` release batch.
/// Every operation's own `(author, log_id)` log is independently sequenced (its `seq_num`/
/// `backlink` chain only ever refers to that same author's prior operations), so a chain walk
/// only ever needs to hold the author fixed across `pop_from`'s hops.
/// The concrete `ChainRing` this buffer wraps: `(author, backlink, log_id)`-keyed entries, one
/// per buffered `Operation<E>`.
type OooChainRing<L, E> = ChainRing<VerifyingKey, Hash, L, Operation<E>>;

#[derive(Clone, Debug)]
pub struct OooBuffer<L, E>
where
    L: LogId,
    E: Extensions,
{
    buffer: Arc<Mutex<OooChainRing<L, E>>>,
}

impl<L, E> Default for OooBuffer<L, E>
where
    L: LogId,
    E: Extensions,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<L, E> OooBuffer<L, E>
where
    L: LogId,
    E: Extensions,
{
    pub fn new() -> Self {
        Self::with_capacity(128)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(ChainRing::with_capacity(capacity))),
        }
    }

    pub async fn len(&self) -> usize {
        self.buffer.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.buffer.lock().await.is_empty()
    }

    pub async fn clear(&self) {
        self.buffer.lock().await.clear();
    }

    // TODO: Use AnyOperation when OperationStore is ready.
    pub async fn process<'a>(
        &self,
        operation: &'a Operation<E>,
        latest_header: Option<&AnyHeader>,
        log_id: &L,
        prune_flag: bool,
    ) -> OooResult<'a, E> {
        // Operation marks a prune point and all operations before that point become redundant,
        // including the ones which would theoretically be "freed" by it.
        //
        // ```text
        //          [0] <- Log in database
        //
        //          [2] <- Operation in ooo-Buffer
        //
        // [4] <- Incoming ooo-Operation
        //  ^
        //  prune_flag=true
        //
        // => Return [4]
        // ```
        //
        // Note that processing [4] will delete [0] in the database. [2] will remain in the
        // ooo-Buffer unused, until it gets evicted.
        if prune_flag {
            return OooResult::InOrder(operation);
        }

        match latest_header {
            Some(latest_header) => {
                if operation.header.seq_num < latest_header.seq_num {
                    // Operation is from _before_ the log frontier and thus redundant.
                    //
                    // Since we assumed that checks for duplicates already taken place, this case
                    // can only occur if the log was pruned and this operation belongs to the
                    // removed log-prefix.
                    //
                    // ```text
                    //          [7] <- [8] <- [9] <- Log in database
                    //           ^
                    //         pruned
                    //
                    // [4] <- Incoming ooo-Operation
                    //
                    // => Return nothing
                    // ```
                    OooResult::Outdated
                } else if latest_header.seq_num == operation.header.seq_num.saturating_sub_signed(1)
                {
                    // Operation is regular, next expected item in log, return it directly.
                    //
                    // ```text
                    // [0] <- [1] <- Log in database
                    //         ^
                    //      Frontier
                    //
                    // [2] <- Incoming Operation
                    //
                    // => Return [2]
                    // ```
                    //
                    // M4-05: this operation itself was never out-of-order, so it was never
                    // pushed into the buffer -- but it may be the missing predecessor of a chain
                    // that *is* sitting there (e.g. [4],[5] arrived before [3], are buffered, and
                    // now [3] arrives directly in-order). Check for and release such a chain
                    // instead of only ever checking the buffer from `push_and_pop_from` (which
                    // this operation never goes through, since it isn't itself out-of-order):
                    // without this, [4]/[5] would stay buffered until *some other, later*
                    // out-of-order arrival happened to re-trigger a buffer check, which is not
                    // guaranteed to ever happen.
                    let freed = self
                        .pop_chain_after(operation.header.verifying_key, operation.hash, log_id)
                        .await;
                    if freed.is_empty() {
                        OooResult::InOrder(operation)
                    } else {
                        let mut ordered = Vec::with_capacity(1 + freed.len());
                        ordered.push(operation.clone());
                        ordered.extend(freed);
                        OooResult::Ordered(ordered)
                    }
                } else {
                    // Operation is _after_ the log frontier and thus out-of-order / can't be
                    // appended to log yet.
                    //
                    // ```text
                    //          [0] <- [1] <- Log in database
                    //
                    // [3] <- Incoming ooo-Operation
                    //
                    // => Push into ooo-Buffer.
                    // ```
                    //
                    // We then check if this item freed any operations in ring-buffer.
                    self.push_and_pop_from(operation, latest_header.backlink, log_id)
                        .await
                }
            }
            None => {
                // There's no log yet. We keep items in the buffer until it runs full or we're
                // building a valid log in memory.
                //
                // ```text
                //          [ ] <- Log in database (empty)
                //
                //          [1] <- Operation in ooo-Buffer
                //
                // [0] <- Incoming ooo-Operation
                //
                // => Return [0, 1]
                // ```
                //
                // Push and then check if this item freed any operations in ring-buffer.
                //
                // We set the expected backlink to `None`, indicating that we are looking for the
                // whole log / from seq_num=0.
                self.push_and_pop_from(operation, None, log_id).await
            }
        }
    }

    /// Pops (without pushing anything new) any chain of buffered operations whose first item's
    /// backlink is `after` -- i.e. releases operations that were waiting on exactly this operation
    /// (M4-05). Used when an operation arrives directly in-order (never itself buffered) but may
    /// still be the missing predecessor for something that *is* sitting in the buffer.
    ///
    /// `author` scopes the chain walk to `operation`'s own author (see the module-level security
    /// note): only that author's own buffered entries can ever be released by this operation.
    async fn pop_chain_after(&self, author: VerifyingKey, after: Hash, log_id: &L) -> Vec<Operation<E>> {
        let mut buffer = self.buffer.lock().await;
        buffer.pop_from(author, Some(after), log_id.clone())
    }

    async fn push_and_pop_from<'a>(
        &self,
        operation: &'a Operation<E>,
        expected_backlink: Option<Hash>,
        log_id: &L,
    ) -> OooResult<'a, E> {
        let mut buffer = self.buffer.lock().await;
        let author = operation.header.verifying_key;

        // Push item to ring-buffer, this will eventually evict old items when full.
        buffer.push(
            author,
            operation.hash,
            operation.header.backlink,
            log_id.clone(),
            operation.clone(),
        );

        // We should check if this item freed any operations in ring-buffer / made them "in-order".
        // The check takes place from the current log frontier (`expected_backlink`) in the
        // database. If it's `None` we don't have any items for the log yet in the database.
        //
        // ```text
        //          [0] <- [1] <- Log in database
        //
        //          [3] <- Operation in ooo-Buffer
        //
        // [2] <- Incoming operation
        //
        // => Return [2, 3]
        // ```
        let result = buffer.pop_from(author, expected_backlink, log_id.clone());
        if result.is_empty() {
            OooResult::OutOfOrder
        } else {
            OooResult::Ordered(result)
        }
    }
}

#[derive(Debug)]
struct ChainRing<A, ID, L, T>
where
    A: Clone + Eq + StdHash,
    ID: Copy + Eq + StdHash,
    L: Clone + Eq + StdHash,
{
    buffer: IndexMap<ChainRingKey<A, ID, L>, ChainRingValue<ID, T>>,
    capacity: usize,
}

#[derive(Debug, Eq, PartialEq, StdHash)]
struct ChainRingKey<A, ID, L>
where
    A: Clone + Eq + StdHash,
    ID: Copy + Eq + StdHash,
    L: Clone + Eq + StdHash,
{
    author: A,
    backlink: Option<ID>,
    log_id: L,
}

#[derive(Debug)]
struct ChainRingValue<ID, T> {
    id: ID,
    item: T,
}

impl<A, ID, L, T> ChainRing<A, ID, L, T>
where
    A: Clone + Eq + StdHash + std::fmt::Debug,
    ID: Copy + Eq + StdHash,
    L: Clone + Eq + StdHash + std::fmt::Debug,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: IndexMap::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, author: A, id: ID, backlink: Option<ID>, log_id: L, item: T) {
        if self.buffer.len() >= self.capacity {
            // Evict the oldest entry (index 0 -- `IndexMap` preserves insertion order and
            // `shift_remove`/`shift_remove_index` keep every remaining entry's relative order, so
            // index 0 is always whichever surviving entry was inserted longest ago). Previously
            // this called `IndexMap::pop`, which removes the *last* entry, the opposite of the
            // "oldest evicted first" behavior documented on `OooBuffer` above (M4-05 review fix).
            if let Some((evicted_key, _evicted_value)) = self.buffer.shift_remove_index(0) {
                warn!(
                    author = ?evicted_key.author,
                    log_id = ?evicted_key.log_id,
                    buffer_len = self.buffer.len(),
                    "ooo buffer full, evicting oldest entry"
                );
            }
        }

        self.buffer.insert(
            ChainRingKey {
                author,
                backlink,
                log_id,
            },
            ChainRingValue { id, item },
        );
    }

    /// Pop all items which have a complete chain from given position, for `author`'s own log
    /// only (see the module-level security note on `OooBuffer`).
    ///
    /// The position is the id of the item _before_ the to-be-popped range:
    ///
    /// ```text
    /// [3] <- [4] <- [5] <- [6]
    ///  ^
    /// pop_from(3) -> [4, 5, 6]
    /// ```
    pub fn pop_from(&mut self, author: A, backlink: Option<ID>, log_id: L) -> Vec<T> {
        let mut result = Vec::new();

        let mut next = ChainRingKey {
            author: author.clone(),
            backlink,
            log_id: log_id.clone(),
        };

        while let Some(ChainRingValue { id, item }) = self.buffer.shift_remove(&next) {
            result.push(item);
            next = ChainRingKey {
                author: author.clone(),
                backlink: Some(id),
                log_id: log_id.clone(),
            };
        }

        result
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn clear(&mut self) {
        self.buffer.clear()
    }
}

#[cfg(test)]
mod tests {
    use super::ChainRing;

    #[test]
    fn push_and_pop_from() {
        let mut ring = ChainRing::with_capacity(64);

        // Form a chain: 4 <- [5] <- [6] <- [7]
        ring.push("alice", 5, Some(4), "test-log", 5);
        ring.push("alice", 6, Some(5), "test-log", 6);
        ring.push("alice", 7, Some(6), "test-log", 7);
        assert_eq!(ring.len(), 3);

        // Try to pop chain range from 3 on, but item [4] is missing.
        assert!(ring.pop_from("alice", Some(3), "test-log").is_empty());

        // Add item [4] to chain: 3 <- [4] <- [5] <- [6] <- [7]
        ring.push("alice", 4, Some(3), "test-log", 4);
        assert_eq!(ring.len(), 4);

        // Pop chain range from 3 on.
        assert_eq!(ring.pop_from("alice", Some(3), "test-log"), vec![4, 5, 6, 7]);
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn log_from_beginning() {
        let mut ring = ChainRing::with_capacity(64);
        ring.push("alice", 0, None, "test-log", 0);
        ring.push("alice", 1, Some(0), "test-log", 1);
        ring.push("alice", 2, Some(1), "test-log", 2);
        assert_eq!(ring.len(), 3);
        assert_eq!(ring.pop_from("alice", None, "test-log"), vec![0, 1, 2]);
    }

    /// M4-05 review fix (F2): the ring evicts the *oldest* entry first, matching the struct's own
    /// doc comment -- previously it evicted the *last-inserted* one (`IndexMap::pop`).
    #[test]
    fn ring_buffer_evicts_oldest_first() {
        let mut ring = ChainRing::with_capacity(2);
        ring.push("alice", 1, Some(0), "test-log", "first");
        ring.push("alice", 2, Some(1), "test-log", "second");
        assert_eq!(ring.len(), 2);

        // Pushing a third item evicts the first ([1], "first"), not the second.
        ring.push("alice", 3, Some(2), "test-log", "third");
        assert_eq!(ring.len(), 2);
        assert!(ring.pop_from("alice", Some(0), "test-log").is_empty());
        assert_eq!(
            ring.pop_from("alice", Some(1), "test-log"),
            vec!["second", "third"]
        );
    }

    /// M4-05 review fix (F1): a `LogId` is often shared by every author on a topic (e.g. the
    /// constant member/key-bundle log id), so the buffer must key on `(author, backlink, log_id)`
    /// -- not just `(backlink, log_id)` -- or one author's forged `backlink` (set to equal a
    /// *different* author's real operation hash) could collide with that other author's chain
    /// lookup and get released alongside their legitimate operations.
    #[test]
    fn cross_author_entries_never_chain_together() {
        let mut ring = ChainRing::with_capacity(64);

        // Alice's own chain: [10] <- [11].
        ring.push("alice", 10, None, "shared-log", "alice-0");
        ring.push("alice", 11, Some(10), "shared-log", "alice-1");

        // Bob (malicious or merely another legitimate author on the same log id) buffers an
        // operation whose `backlink` happens to equal alice's op [10]'s hash -- same key
        // components (`backlink`, `log_id`) alice's own chain lookup would use, differing only by
        // author.
        ring.push("bob", 99, Some(10), "shared-log", "bob-forged");
        assert_eq!(ring.len(), 3);

        // Alice's own pop_from must only ever release alice's entries.
        let released = ring.pop_from("alice", None, "shared-log");
        assert_eq!(released, vec!["alice-0", "alice-1"]);

        // Bob's entry is untouched -- neither released early nor overwritten -- and pops
        // correctly under bob's own author.
        assert_eq!(ring.len(), 1);
        assert_eq!(
            ring.pop_from("bob", Some(10), "shared-log"),
            vec!["bob-forged"]
        );
        assert!(ring.is_empty());
    }
}
