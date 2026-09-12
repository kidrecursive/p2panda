// SPDX-License-Identifier: MIT OR Apache-2.0

use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::{Stream, StreamExt};
use p2panda_core::Topic;
use p2panda_sync::FromSync;
use ractor::{ActorRef, call};
use thiserror::Error;
use tokio::sync::{broadcast, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::sync::actors::{ToSyncManager, ToTopicManager};

/// Handle to a sync stream.
///
/// The stream can be used to publish messages or to request a subscription.
#[derive(Debug)]
pub struct SyncHandle<M, E>
where
    M: Clone + Send + 'static,
    E: Clone + Send + 'static,
{
    topic: Topic,
    manager_ref: ActorRef<ToSyncManager<M, E>>,
    topic_manager_ref: ActorRef<ToTopicManager<M>>,
    has_closed: AtomicBool,
}

impl<M, E> SyncHandle<M, E>
where
    M: Clone + Send + 'static,
    E: Clone + Send + 'static,
{
    pub(crate) fn new(
        topic: Topic,
        manager_ref: ActorRef<ToSyncManager<M, E>>,
        topic_manager_ref: ActorRef<ToTopicManager<M>>,
    ) -> Self {
        Self {
            topic,
            manager_ref,
            topic_manager_ref,
            has_closed: AtomicBool::new(false),
        }
    }

    /// Publishes a message to the stream.
    pub fn publish(&self, data: M) -> Result<(), SyncHandleError<M, E>> {
        // This would likely be a critical failure for this stream handle, since we are unable to
        // send messages to the sync manager.
        self.topic_manager_ref
            .send_message(ToTopicManager::Publish(data))
            .map_err(Box::new)?;
        Ok(())
    }

    /// Subscribes to the stream.
    ///
    /// The returned `SyncSubscription` provides a means of receiving messages from
    /// the stream.
    pub async fn subscribe(&self) -> Result<SyncSubscription<E>, SyncHandleError<M, E>> {
        if let Some(stream) =
            call!(self.manager_ref, ToSyncManager::Subscribe, self.topic).map_err(Box::new)?
        {
            Ok(SyncSubscription::<E>::new(self.topic, stream))
        } else {
            Err(SyncHandleError::StreamNotFound)
        }
    }

    /// Returns the topic of the stream.
    pub fn topic(&self) -> Topic {
        self.topic
    }

    /// Manually starts a sync session with the given node.
    ///
    /// If there's no transport information for this node this action will fail.
    ///
    /// square-tower fork addition (`square-tower/main`, D3-k in the downstream project's
    /// `decisions.md`): made available outside test builds so applications can trigger a resync
    /// on their own schedule, independent of gossip's HyParView active-view churn (which can
    /// permanently end a topic's sync session with a still-reachable, still-allowed peer with no
    /// automatic recovery -- `p2panda-net/src/sync/actors/topic_manager.rs`'s retry path only
    /// fires on `ActorFailed`, never on a session that ended gracefully after `GossipEvent::
    /// NeighbourDown`). Upstream PR draft: `docs/upstream/p2panda-manual-resync.md` in that
    /// project's repo. This method's behavior is unchanged for existing (test) callers -- only
    /// its visibility widened.
    pub fn initiate_session(&self, node_id: crate::NodeId) {
        self.manager_ref
            .send_message(ToSyncManager::InitiateSync(self.topic, node_id))
            .unwrap();
    }

    /// Explicitly resyncs with the given node, REPLACING a live session for this topic if one
    /// exists and its initial catch-up phase has already finished.
    ///
    /// square-tower fork addition (`square-tower/main`, D3-s in the downstream project's
    /// `decisions.md`): a `TopicLogSync` session's offered log set is frozen once it resolves
    /// (D24-13) -- any `(topic, author, log)` association made after that point is unreachable to
    /// that peer for the rest of that session's lifetime, and `initiate_session`'s `Initiate`
    /// dedupe skips spawning a new session for as long as any session with that peer stays open.
    /// This method exists for exactly that case: closing an already-caught-up live session and
    /// starting a fresh one (full log-set resolve + live mode), so a late association becomes
    /// reachable within one call. If the peer's session is still in its initial catch-up, this is a
    /// no-op (logged `debug!`, not retried here -- the caller is expected to be a periodic task
    /// that will call again on its own schedule). If there's no live session at all, this behaves
    /// exactly like `initiate_session`. Runs the same `ConnectionAuthoriser` check
    /// `initiate_session` runs, unconditionally -- no bypass. Intended for use ONLY by the node's
    /// own periodic resync task (`p2panda::streams::StreamPublisher::resync`/`p2panda::spaces::
    /// Space::resync`), never the gossip-driven path, which keeps using `initiate_session`'s
    /// existing dedupe unchanged. Upstream PR draft:
    /// `docs/upstream/p2panda-resync-replaces-session.md`.
    pub fn resync(&self, node_id: crate::NodeId) {
        self.manager_ref
            .send_message(ToSyncManager::Resync(self.topic, node_id))
            .unwrap();
    }

    /// Close the associated sync session gracefully.
    ///
    /// This method can be awaited to ensure that all sync-related state has been cleaned up.
    pub async fn close(&self) -> Result<(), SyncHandleError<M, E>> {
        let (reply, reply_rx) = oneshot::channel();

        self.manager_ref
            .send_message(ToSyncManager::Close(self.topic, reply))
            .map_err(Box::new)?;

        // Await the termination completion signal.
        let _ = reply_rx.await;
        self.has_closed.store(true, Ordering::Relaxed);

        Ok(())
    }
}

impl<M, E> Drop for SyncHandle<M, E>
where
    M: Clone + Send + 'static,
    E: Clone + Send + 'static,
{
    fn drop(&mut self) {
        let (reply, _reply_rx) = oneshot::channel();

        // Sending `ToSyncManager::Close` twice for the same topic in quick succession can cause
        // an error in the `TopicManager` actor; this occurs if a new topic stream is created
        // immediately after sending the `Close` instruction.
        //
        // The error case is clearer if we consider this example of the TopicManager queue:
        //
        // Close     -> cleans up all state for the topic
        // Create    -> sets up state for the topic
        // Close     -> cleans up all state for the topic
        // Subscribe -> returns an error because the expected state doesn't exist
        //
        // Only send the message here if a graceful closure has not been initiated from a higher
        // level.
        if !self.has_closed.load(Ordering::Relaxed) {
            // Ignore error here as the actor might already be dropped.
            let _ = self
                .manager_ref
                .send_message(ToSyncManager::Close(self.topic, reply));
        }
    }
}

/// Handle to a sync subscription.
///
/// The stream can be used to receive messages from the stream.
pub struct SyncSubscription<E> {
    topic: Topic,
    // Messages sent directly from the topic manager.
    from_sync_rx: BroadcastStream<FromSync<E>>,
}

impl<E> SyncSubscription<E>
where
    E: Clone + Send + 'static,
{
    pub(crate) fn new(topic: Topic, from_sync_rx: broadcast::Receiver<FromSync<E>>) -> Self {
        Self {
            topic,
            from_sync_rx: BroadcastStream::new(from_sync_rx),
        }
    }

    /// Returns the topic of the stream.
    pub fn topic(&self) -> Topic {
        self.topic
    }
}

impl<E> Stream for SyncSubscription<E>
where
    E: Clone + Send + 'static,
{
    type Item = Result<FromSync<E>, BroadcastStreamRecvError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.from_sync_rx.poll_next_unpin(cx)
    }
}

#[derive(Debug, Error)]
pub enum SyncHandleError<M, E> {
    /// Messaging with internal actor via RPC failed.
    #[error(transparent)]
    ActorRpc(#[from] Box<ractor::RactorErr<ToSyncManager<M, E>>>),

    #[error(transparent)]
    Publish(#[from] Box<ractor::MessagingErr<ToTopicManager<M>>>),

    #[error("no stream exists for the given topic")]
    StreamNotFound,

    #[error(transparent)]
    Close(#[from] Box<ractor::MessagingErr<ToSyncManager<M, E>>>),
}
