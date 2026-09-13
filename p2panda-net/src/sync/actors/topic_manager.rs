// SPDX-License-Identifier: MIT OR Apache-2.0

//! Topic manager actor.
//!
//! The topic manager holds state for all sync sessions associated with a single topic. It provides
//! a means of initating new sync sessions, accepting inbound sync sessions and publishing messages
//! to all active sync sessions for the associated topic.
//!
//! A separate topic manager actor is spawned by the sync manager for each topic of interest.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures_util::{Sink, SinkExt, StreamExt};
use iroh::endpoint::Connection;
use p2panda_core::{Topic, VerifyingKey};
use p2panda_sync::manager::SessionTopicMap;
use p2panda_sync::traits::Manager as SyncManagerTrait;
use p2panda_sync::{FromSync, SessionConfig, ToSync};
use ractor::thread_local::{ThreadLocalActor, ThreadLocalActorSpawner};
use ractor::{ActorId, ActorProcessingErr, ActorRef, SupervisionEvent};
use tokio::sync::{broadcast, oneshot};
use tokio::time::Duration;
use tracing::{debug, info, warn};

use crate::connection_authoriser::{ConnectionAuthoriser, ConnectionAuthoriserEvent};
use crate::iroh_endpoint::Endpoint;
use crate::sync::actors::poller::{SyncPoller, ToSyncPoller};
use crate::sync::actors::session::{SyncSession, SyncSessionId, SyncSessionMessage};
use crate::utils::ShortFormat;
use crate::{NodeId, ProtocolId};

const RETRY_RATE: Duration = Duration::from_secs(5);

type SessionSink<M> = Pin<
    Box<
        dyn Sink<
                ToSync<<M as SyncManagerTrait<Topic>>::Message>,
                Error = <M as SyncManagerTrait<Topic>>::Error,
            >,
    >,
>;

/// square-tower fork addition (D3-u, M4-21): per-session resolved-log baseline snapshots, shared
/// with the background listener task spawned in `pre_start` (see `TopicManagerState::session_logs`).
type SessionLogs<M> = Arc<
    Mutex<
        HashMap<SyncSessionId, BTreeMap<VerifyingKey, Vec<<M as SyncManagerTrait<Topic>>::LogId>>>,
    >,
>;

#[derive(Debug)]
pub enum ToTopicManager<T> {
    /// Initiate a sync session with this peer over the given topic
    Initiate {
        node_id: NodeId,
        topic: Topic,
        live_mode: bool,
    },

    /// Accept a sync session on this connection.
    Accept {
        node_id: NodeId,
        topic: Topic,
        live_mode: bool,
        connection: Connection,
    },

    /// Retry sync with this peer after a failed session.
    Retry { node_id: NodeId, live_mode: bool },

    /// Send newly published data to all sync sessions running over the given topic.
    Publish(T),

    /// Close all active sync sessions running over the given topic. This essentially shuts down
    /// the whole manager.
    CloseAll(oneshot::Sender<()>),

    /// Close all active sync sessions running with the given node id.
    Close {
        node_id: NodeId,
        reply: oneshot::Sender<()>,
    },

    /// square-tower fork addition (D3-s): explicitly resync with this peer, REPLACING a live
    /// session for this (peer, topic) if one exists and has already finished its initial catch-up
    /// (`Manager::is_catch_up_finished`) -- close it, wait for it to actually terminate, then
    /// initiate a fresh session (full log-set resolve + live mode). If a catch-up is still in
    /// progress for the current session, this is a no-op (logged). If there's no live session,
    /// this behaves exactly like `Initiate`. Sent ONLY by the node's own periodic resync
    /// (`SyncHandle::resync`, via `ToSyncManager::Resync`), never by the gossip-driven path, which
    /// keeps using plain `Initiate` and its existing dedupe unchanged.
    Resync {
        node_id: NodeId,
        topic: Topic,
        live_mode: bool,
    },

    /// square-tower fork addition (D3-u, M4-21): a new association was pushed for this topic
    /// (`TopicStore::associate`'s `is_new` case, via `Manager::subscribe_new_associations`).
    /// Triggers an immediate structural resync check against every currently active peer, instead
    /// of waiting for the next `sync.resync_interval` tick.
    AssociationChanged,
}

pub struct TopicManagerState<M>
where
    M: SyncManagerTrait<Topic>,
{
    topic: Topic,
    manager: M,
    protocol_id: ProtocolId,
    session_topic_map: SessionTopicMap<Topic, SessionSink<M>>,
    node_session_map: HashMap<NodeId, HashSet<SyncSessionId>>,
    active_sync_set: HashSet<NodeId>,
    actor_session_id_map: HashMap<ActorId, SyncSessionId>,
    next_session_id: SyncSessionId,
    sync_poller_actor: ActorRef<ToSyncPoller>,
    endpoint: Endpoint,
    pool: ThreadLocalActorSpawner,

    /// square-tower fork addition (D3-s): per-session, whether `Manager::is_catch_up_finished` has
    /// been observed. Populated by a background task (spawned in `pre_start`) subscribed to the
    /// same broadcast channel the poller forwards session events on -- no new event source, just
    /// an additional listener. Missing/`false` is the safe default: a resync treats "we haven't
    /// seen catch-up finish yet" the same as "still in catch-up".
    session_catch_up: Arc<Mutex<HashMap<SyncSessionId, bool>>>,

    /// square-tower fork addition (D3-s): node ids whose live session(s) are currently being
    /// closed as part of an in-progress resync replacement, mapped to the `live_mode` the fresh
    /// session should use and the set of session ids we're still waiting to see actually
    /// terminate. A second `Resync` for a node already in this map is a no-op (at most one
    /// replacement in flight per (peer, topic) at a time). Cleared without re-initiating if the
    /// node leaves `active_sync_set` (gossip `NeighbourDown`/`EndSync`, or `Close`/`CloseAll`)
    /// before the old session(s) actually terminate -- see the M4-16 review fix in
    /// `handle_supervisor_evt` and the `Close`/`CloseAll` handlers below.
    ///
    /// square-tower fork addition (D3-u, M4-21 fix): a peer can legitimately have more than one
    /// concurrent session at resync time (D3-k's own dedupe comment documents the gossip race that
    /// produces this); closing all of them and re-`Initiate`-ing on the *first* termination raced
    /// `Initiate`'s "skip if another session for this node still runs" dedupe against the *other*
    /// stale session not having terminated yet -- silently dropping the peer forever (found by
    /// M4-21's own `Resync` firing immediately on the very first association, instead of D3-s's
    /// 30s tick, which made this latent race fire deterministically on ordinary two-node sync
    /// instead of only rarely). Fixed by waiting for every session named in the replacement to
    /// terminate before resuming.
    pending_resync: HashMap<NodeId, (bool, HashSet<SyncSessionId>)>,

    /// square-tower fork addition (D3-s, M4-16 review): used to re-run the identical
    /// `ConnectionAuthoriser` check `ToSyncManager::Resync`/`InitiateSync` already ran, at the
    /// point a deferred resync replacement actually re-`Initiate`s (`handle_supervisor_evt`) --
    /// that check ran once, before the old session was asked to close, and authorisation state can
    /// change in the window between that check and the old session's actual termination. Cloned
    /// from the same `ConnectionAuthoriser` the owning `SyncManager` holds (passed in at spawn).
    connection_authoriser: ConnectionAuthoriser,

    /// square-tower fork addition (D3-u, M4-21): per-session resolved-log baseline snapshot, as
    /// reported by that session's own `Manager::resolved_logs_from_event` (the fork's
    /// `LogsResolved` event). Populated by the same background task that already tracks
    /// `session_catch_up` (same broadcast channel, additional match arm) -- not a new event
    /// source. A session missing from this map has not yet resolved its baseline and is treated
    /// as "not stale" (deferred to the next check) by the `Resync` handler.
    session_logs: SessionLogs<M>,
}

#[derive(Debug)]
pub struct TopicManager<M> {
    _marker: PhantomData<M>,
}

impl<M> Default for TopicManager<M> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<M> ThreadLocalActor for TopicManager<M>
where
    M: SyncManagerTrait<Topic> + Send + 'static,
    M::LogId: Send + Sync + 'static,
{
    type State = TopicManagerState<M>;

    type Msg = ToTopicManager<M::Message>;

    type Arguments = (
        ProtocolId,
        Topic,
        M::Args,
        broadcast::Sender<FromSync<M::Event>>,
        Endpoint,
        ConnectionAuthoriser,
    );

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let (protocol_id, topic, config, sender, endpoint, connection_authoriser) = args;
        let pool = ThreadLocalActorSpawner::new();

        let mut manager = M::from_args(config);
        let event_stream = manager.subscribe();

        // square-tower fork addition (D3-s): subscribe to the same broadcast channel the poller
        // below forwards session events on (before `sender` moves into the poller's arguments) so
        // we can track, per session id, whether `Manager::is_catch_up_finished` has fired -- this
        // reads events already flowing through the actor, it does not add a new event source.
        let session_catch_up = Arc::new(Mutex::new(HashMap::<SyncSessionId, bool>::new()));
        // square-tower fork addition (D3-u, M4-21): per-session resolved-log baseline, populated
        // by the same listener below (additional match arm, no new event source).
        let session_logs = Arc::new(Mutex::new(HashMap::<
            SyncSessionId,
            BTreeMap<VerifyingKey, Vec<M::LogId>>,
        >::new()));
        {
            let mut event_rx = sender.subscribe();
            let session_catch_up = session_catch_up.clone();
            let session_logs = session_logs.clone();
            tokio::spawn(async move {
                loop {
                    match event_rx.recv().await {
                        Ok(event) => {
                            if M::is_catch_up_finished(&event.event) {
                                session_catch_up
                                    .lock()
                                    .expect("session_catch_up mutex poisoned")
                                    .insert(event.session_id, true);
                            }
                            if let Some(logs) = M::resolved_logs_from_event(&event.event) {
                                session_logs
                                    .lock()
                                    .expect("session_logs mutex poisoned")
                                    .insert(event.session_id, logs);
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        }

        // square-tower fork addition (D3-u, M4-21): push notification for new associations on
        // this topic (`Manager::subscribe_new_associations`, itself delegating to
        // `TopicStore::associate`'s sole real association choke point) -- posts `AssociationChanged`
        // to trigger an immediate structural resync check, instead of waiting for the periodic
        // `sync.resync_interval` tick (which remains as a bounded fallback).
        {
            let myself = myself.clone();
            let mut new_associations = manager.subscribe_new_associations(&topic);
            tokio::spawn(async move {
                while new_associations.next().await.is_some() {
                    if myself
                        .send_message(ToTopicManager::AssociationChanged)
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }

        // The sync poller actor lives as long as the manager and only terminates due to the
        // manager actor itself terminating.
        let (sync_poller_actor, _) =
            SyncPoller::spawn_linked(None, (event_stream, sender), myself.into(), pool.clone())
                .await?;

        Ok(TopicManagerState {
            topic,
            manager,
            protocol_id,
            session_topic_map: SessionTopicMap::default(),
            node_session_map: HashMap::new(),
            active_sync_set: HashSet::new(),
            next_session_id: 0,
            actor_session_id_map: HashMap::new(),
            sync_poller_actor,
            endpoint,
            pool,
            session_catch_up,
            pending_resync: HashMap::new(),
            connection_authoriser,
            session_logs,
        })
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // Drain the sync poller to ensure that all sync session messages are forwarded before it
        // is shut down. A timeout is included to ensure that the drain call cannot wait forever.
        state
            .sync_poller_actor
            .drain_and_wait(Some(Duration::from_millis(5000)))
            .await?;

        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ToTopicManager::Initiate {
                node_id,
                topic,
                live_mode,
            } => {
                state.active_sync_set.insert(node_id);

                // square-tower fork addition (D3-k): don't spawn a second concurrent session for
                // a peer+topic pair that already has one running. Without this, a burst of
                // `Initiate` calls for the same peer (e.g. gossip's `Joined`/`NeighbourUp` firing
                // for several nodes at once, or the node-side periodic resync racing an
                // in-flight session) can create multiple simultaneous sessions for the same
                // (node_id, topic); each session delivers a *different log's* operations with no
                // cross-log ordering guarantee between them, which can violate a cross-log causal
                // dependency (e.g. `p2panda-spaces`'s `SpaceMembership` message referencing a
                // groups-log operation that a *different*, concurrently-running session hasn't
                // delivered yet) -- the dependent operation then fails once, non-retryably, with
                // no error surfaced anywhere above the spaces manager (M4-07,
                // `docs/upstream/p2panda-manual-resync.md`). `Retry` already had this exact guard
                // (`current_sessions.is_empty()` below); `Initiate` now matches it.
                let current_sessions = state
                    .node_session_map
                    .get(&node_id)
                    .cloned()
                    .unwrap_or_default();
                if !current_sessions.is_empty() {
                    debug!(
                        node_id = %state.endpoint.node_id().fmt_short(),
                        remote_node_id = %node_id.fmt_short(),
                        topic = %topic.fmt_short(),
                        %live_mode,
                        "skip initiate sync: other sync sessions already running with this node"
                    );
                    return Ok(());
                }

                debug!(
                    node_id = %state.endpoint.node_id().fmt_short(),
                    remote_node_id = %node_id.fmt_short(),
                    topic = %topic.fmt_short(),
                    %live_mode,
                    "initiate sync session"
                );
                let config = SessionConfig {
                    topic,
                    remote: node_id,
                    live_mode,
                };
                let (actor_ref, _) = SyncSession::<M::Protocol>::spawn_linked(
                    None,
                    (state.endpoint.clone(),),
                    myself.clone().into(),
                    state.pool.clone(),
                )
                .await?;
                let (session_id, protocol) =
                    Self::new_session(state, actor_ref.get_id(), node_id, topic, config).await;

                actor_ref.send_message(SyncSessionMessage::Initiate {
                    node_id,
                    topic,
                    session_id,
                    protocol,
                    protocol_id: state.protocol_id.clone(),
                })?;
            }
            ToTopicManager::Retry { node_id, live_mode } => {
                // If this node was removed from the active sync set we skip retrying.
                if !state.active_sync_set.contains(&node_id) {
                    info!(
                        remote = %node_id.fmt_short(),
                        topic = %state.topic.fmt_short(),
                        %live_mode,
                        "skip re-initiate sync: node no longer in active set"
                    );
                    return Ok(());
                };

                let current_sessions = state
                    .node_session_map
                    .get(&node_id)
                    .cloned()
                    .unwrap_or_default();

                // If there's another session running then we don't need to re-initiate sync.
                if !current_sessions.is_empty() {
                    debug!(
                        remote = %node_id.fmt_short(),
                        topic = %state.topic.fmt_short(),
                        %live_mode,
                        "skip re-initiate sync: other sync sessions already running"
                    );
                    return Ok(());
                }

                debug!(
                    remote = %node_id.fmt_short(),
                    topic = %state.topic.fmt_short(),
                    %live_mode,
                    "re-initiate sync after failed session"
                );

                let config = SessionConfig {
                    topic: state.topic,
                    remote: node_id,
                    live_mode,
                };
                let (actor_ref, _) = SyncSession::<M::Protocol>::spawn_linked(
                    None,
                    (state.endpoint.clone(),),
                    myself.clone().into(),
                    state.pool.clone(),
                )
                .await?;

                let (session_id, protocol) =
                    Self::new_session(state, actor_ref.get_id(), node_id, state.topic, config)
                        .await;

                actor_ref.send_message(SyncSessionMessage::Initiate {
                    node_id,
                    topic: state.topic,
                    session_id,
                    protocol,
                    protocol_id: state.protocol_id.clone(),
                })?;
            }
            ToTopicManager::Accept {
                node_id,
                connection,
                topic,
                live_mode,
            } => {
                debug!(
                    node_id = %state.endpoint.node_id().fmt_short(),
                    remote = %node_id.fmt_short(),
                    topic = %topic.fmt_short(),
                    %live_mode,
                    "accept sync session"
                );

                let config = SessionConfig {
                    topic,
                    remote: node_id,
                    live_mode,
                };
                let (actor_ref, _) = SyncSession::<M::Protocol>::spawn_linked(
                    None,
                    (state.endpoint.clone(),),
                    myself.clone().into(),
                    state.pool.clone(),
                )
                .await?;
                let (session_id, protocol) =
                    Self::new_session(state, actor_ref.get_id(), node_id, topic, config).await;

                actor_ref.send_message(SyncSessionMessage::Accept {
                    connection,
                    topic,
                    session_id,
                    protocol,
                })?;
            }
            ToTopicManager::Publish(data) => {
                // Get a handle onto any sync sessions running over the subscription topic and
                // forward on the data.
                let session_ids = state.session_topic_map.sessions(&state.topic);

                for id in session_ids {
                    let handle = state
                        .session_topic_map
                        .sender_mut(id)
                        .expect("session handle exists");
                    let _ = handle.send(ToSync::Payload(data.clone())).await;
                }
            }
            ToTopicManager::CloseAll(reply) => {
                // Get a handle onto any sync sessions running over the subscription topic and send
                // a Close message. The session will send a close message to the remote then
                // immediately drop the session.
                let session_ids = state.session_topic_map.sessions(&state.topic);

                for id in session_ids {
                    let handle = state
                        .session_topic_map
                        .sender_mut(id)
                        .expect("session handle exists");
                    let _ = handle.send(ToSync::Close).await;
                }

                for node_id in state.active_sync_set.drain() {
                    debug!(
                        topic = state.topic.fmt_short(),
                        "removed node from active sync set: {}",
                        node_id.fmt_short()
                    );
                }

                // square-tower fork addition (D3-s, M4-16 review): a pending resync replacement
                // for any of these nodes must not resurrect a session once every session is being
                // torn down.
                if !state.pending_resync.is_empty() {
                    debug!(
                        topic = state.topic.fmt_short(),
                        "cancelled {} pending resync(s) on CloseAll",
                        state.pending_resync.len()
                    );
                    state.pending_resync.clear();
                }

                // The receiver may have been dropped immediately if the caller is not interested
                // in awaiting the termination signal, so we ignore any potential error here.
                let _ = reply.send(());
            }
            ToTopicManager::Close { node_id, reply } => {
                if state.active_sync_set.remove(&node_id) {
                    debug!(
                        topic = state.topic.fmt_short(),
                        "removed node from active sync set: {}",
                        node_id.fmt_short()
                    );
                };

                // square-tower fork addition (D3-s, M4-16 review): this node is being closed
                // (gossip `NeighbourDown` -> `EndSync`, or an explicit close) -- a resync
                // replacement pending for it must not resurrect a session once its old one
                // actually terminates. `handle_supervisor_evt`'s own `active_sync_set` gate is a
                // second, independent line of defence for the same race; this one fires
                // immediately, without waiting for termination.
                if state.pending_resync.remove(&node_id).is_some() {
                    debug!(
                        remote_node_id = %node_id.fmt_short(),
                        topic = state.topic.fmt_short(),
                        "cancelled pending resync: node is being closed"
                    );
                }

                let node_sessions = state.node_session_map.get(&node_id).cloned();

                if let Some(node_sessions) = node_sessions {
                    let topic_sessions = state.session_topic_map.sessions(&state.topic);

                    for id in topic_sessions.intersection(&node_sessions) {
                        let session_topic =
                            state.session_topic_map.topic(*id).expect("topic to exist");

                        if &state.topic != session_topic {
                            continue;
                        }

                        let handle = state
                            .session_topic_map
                            .sender_mut(*id)
                            .expect("session handle exists");

                        let _ = handle.send(ToSync::Close).await;
                    }
                };

                let _ = reply.send(());
            }
            ToTopicManager::Resync {
                node_id,
                topic,
                live_mode,
            } => {
                if state.pending_resync.contains_key(&node_id) {
                    info!(
                        node_id = %state.endpoint.node_id().fmt_short(),
                        remote_node_id = %node_id.fmt_short(),
                        topic = %topic.fmt_short(),
                        "skip resync: a replacement is already in flight for this node"
                    );
                    return Ok(());
                }

                let current_sessions = state
                    .node_session_map
                    .get(&node_id)
                    .cloned()
                    .unwrap_or_default();

                if current_sessions.is_empty() {
                    info!(
                        node_id = %state.endpoint.node_id().fmt_short(),
                        remote_node_id = %node_id.fmt_short(),
                        topic = %topic.fmt_short(),
                        %live_mode,
                        "resync: no live session, initiating"
                    );
                    myself.send_message(ToTopicManager::Initiate {
                        node_id,
                        topic,
                        live_mode,
                    })?;
                    return Ok(());
                }

                let catch_up_in_progress = {
                    let session_catch_up = state
                        .session_catch_up
                        .lock()
                        .expect("session_catch_up mutex poisoned");
                    current_sessions
                        .iter()
                        .any(|id| !session_catch_up.get(id).copied().unwrap_or(false))
                };

                if catch_up_in_progress {
                    info!(
                        node_id = %state.endpoint.node_id().fmt_short(),
                        remote_node_id = %node_id.fmt_short(),
                        topic = %topic.fmt_short(),
                        "skip resync: catch-up in progress"
                    );
                    return Ok(());
                }

                // square-tower fork addition (D3-u, M4-21): event-driven, structurally-gated
                // resync -- replace a session only when the topic's actual resolved (author, log)
                // set differs from what that session resolved at its own start (its `LogsResolved`
                // baseline). A session missing from `session_logs` has not yet resolved its
                // baseline and is treated as "not stale" (deferred to the next tick/association),
                // not as drifted.
                let fresh = state.manager.resolved_logs(&topic).await;
                let stale: Vec<SyncSessionId> = {
                    let session_logs = state
                        .session_logs
                        .lock()
                        .expect("session_logs mutex poisoned");
                    current_sessions
                        .iter()
                        .filter(|id| {
                            session_logs
                                .get(id)
                                .is_some_and(|baseline| baseline != &fresh)
                        })
                        .copied()
                        .collect()
                };

                if stale.is_empty() {
                    debug!(
                        node_id = %state.endpoint.node_id().fmt_short(),
                        remote_node_id = %node_id.fmt_short(),
                        topic = %topic.fmt_short(),
                        "skip resync: log set unchanged"
                    );
                    return Ok(());
                }

                info!(
                    node_id = %state.endpoint.node_id().fmt_short(),
                    remote_node_id = %node_id.fmt_short(),
                    topic = %topic.fmt_short(),
                    %live_mode,
                    "resync: replacing live session"
                );
                state
                    .pending_resync
                    .insert(node_id, (live_mode, stale.iter().copied().collect()));

                for id in &stale {
                    if let Some(handle) = state.session_topic_map.sender_mut(*id) {
                        let _ = handle.send(ToSync::Close).await;
                    }
                }
            }
            ToTopicManager::AssociationChanged => {
                // square-tower fork addition (D3-u, M4-21): a new association was pushed for this
                // topic -- check every currently active peer for structural drift right away,
                // instead of waiting for the next `sync.resync_interval` tick. `Resync`'s own
                // structural check (above) still gates whether anything is actually replaced.
                for node_id in state.active_sync_set.iter().copied().collect::<Vec<_>>() {
                    myself.send_message(ToTopicManager::Resync {
                        node_id,
                        topic: state.topic,
                        live_mode: true,
                    })?;
                }
            }
        }

        Ok(())
    }

    // Handle supervision events from sync session and poller actors.
    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SupervisionEvent::ActorTerminated(actor_cell, _, _) => {
                match state.actor_session_id_map.remove(&actor_cell.get_id()) {
                    Some(session_id) => {
                        info!(
                            %session_id,
                            topic = state.topic.fmt_short(),
                            "sync session terminated"
                        );

                        // square-tower fork addition (D3-s): capture which node owned this
                        // session before `drop_session` clears the mapping, so a resync
                        // replacement pending on this exact session can be resumed now that the
                        // old session has actually terminated (not merely been asked to close).
                        let owner =
                            state
                                .node_session_map
                                .iter()
                                .find_map(|(node_id, sessions)| {
                                    sessions.contains(&session_id).then_some(*node_id)
                                });

                        Self::drop_session(state, session_id);
                        state
                            .session_catch_up
                            .lock()
                            .expect("session_catch_up mutex poisoned")
                            .remove(&session_id);
                        state
                            .session_logs
                            .lock()
                            .expect("session_logs mutex poisoned")
                            .remove(&session_id);

                        if let Some(node_id) = owner
                            && let Some(live_mode) =
                                Self::record_pending_resync_termination(state, node_id, session_id)
                        {
                            Self::resume_pending_resync(
                                &myself,
                                state,
                                node_id,
                                live_mode,
                                "terminated",
                            )
                            .await;
                        }
                    }
                    None => {
                        let actor_id = actor_cell.get_id();
                        debug!(
                            %actor_id,
                            topic = state.topic.fmt_short(),
                            "sync poller terminated"
                        );
                    }
                }
            }
            SupervisionEvent::ActorFailed(actor_cell, err) => {
                match state.actor_session_id_map.remove(&actor_cell.get_id()) {
                    Some(session_id) => {
                        warn!(
                            %session_id,
                            topic = state.topic.fmt_short(),
                            "sync session failed: {err}"
                        );

                        // Retrieve the node id and current sessions from the node session map.
                        let Some(remote_node_id) =
                            state
                                .node_session_map
                                .iter()
                                .find_map(|(node_id, sessions)| {
                                    if sessions.contains(&session_id) {
                                        Some(*node_id)
                                    } else {
                                        None
                                    }
                                })
                        else {
                            // If it wasn't present then it means we no longer want to sync with
                            // this node, clear up any session state and return.
                            Self::drop_session(state, session_id);
                            state
                                .session_catch_up
                                .lock()
                                .expect("session_catch_up mutex poisoned")
                                .remove(&session_id);
                            state
                                .session_logs
                                .lock()
                                .expect("session_logs mutex poisoned")
                                .remove(&session_id);
                            return Ok(());
                        };

                        // Clear up any state from the failed session.
                        Self::drop_session(state, session_id);
                        state
                            .session_catch_up
                            .lock()
                            .expect("session_catch_up mutex poisoned")
                            .remove(&session_id);
                        state
                            .session_logs
                            .lock()
                            .expect("session_logs mutex poisoned")
                            .remove(&session_id);

                        // square-tower fork addition (D3-s): a resync replacement's `Close` can
                        // race the old session into failing instead of terminating gracefully
                        // (e.g. the remote closes the connection first). Either way, the old
                        // session is gone, so resume the pending replacement the same as the
                        // graceful path -- and skip the normal failure-retry timer, since we're
                        // already re-initiating immediately.
                        if let Some(live_mode) = Self::record_pending_resync_termination(
                            state,
                            remote_node_id,
                            session_id,
                        ) {
                            Self::resume_pending_resync(
                                &myself,
                                state,
                                remote_node_id,
                                live_mode,
                                "failed",
                            )
                            .await;
                            return Ok(());
                        }

                        // If this node was removed from the active sync set we skip retrying.
                        if !state.active_sync_set.contains(&remote_node_id) {
                            info!(
                                remote = remote_node_id.fmt_short(),
                                topic = state.topic.fmt_short(),
                                "skip re-initiate sync: node no longer in active set"
                            );

                            return Ok(());
                        };

                        // Send a retry message to the actor after a 5 second delay.
                        let _ = myself
                            .send_after(RETRY_RATE, move || {
                                ToTopicManager::Retry {
                                    node_id: remote_node_id,
                                    // TODO: For now we default to live-mode is true but we should
                                    // rather retrieve this state from the failed sync session.
                                    live_mode: true,
                                }
                            })
                            .await;
                    }
                    None => {
                        let actor_id = actor_cell.get_id();
                        warn!(
                            %actor_id,
                            topic = state.topic.fmt_short(),
                            "sync poller failed: {err}"
                        );
                    }
                }
            }
            _ => (),
        }

        Ok(())
    }
}

impl<M> TopicManager<M>
where
    M: SyncManagerTrait<Topic> + Send + 'static,
    <M as SyncManagerTrait<Topic>>::Error: StdError + Send + Sync + 'static,
    M::LogId: Send + Sync + 'static,
{
    /// Initiate a session and update related manager state mappings.
    async fn new_session(
        state: &mut TopicManagerState<M>,
        actor_id: ActorId,
        node_id: NodeId,
        topic: Topic,
        config: SessionConfig<Topic>,
    ) -> (u64, <M as SyncManagerTrait<Topic>>::Protocol) {
        let session_id: SyncSessionId = state.next_session_id;
        state.next_session_id += 1;

        let session = state.manager.session(session_id, &config).await;

        let session_handle = state
            .manager
            .session_handle(session_id)
            .await
            .expect("we just created this session");

        // Register the session on the manager state.
        //
        // NOTE: We don't distinguish between "accepting" and "accepted" sync sessions as in both
        // cases the topic is known thanks to the topic handshake already having been performed.
        state
            .session_topic_map
            .insert_with_topic(session_id, topic, session_handle);

        // Associate the session with the given node id on manager state.
        state
            .node_session_map
            .entry(node_id)
            .or_default()
            .insert(session_id);

        state.actor_session_id_map.insert(actor_id, session_id);

        (session_id, session)
    }

    /// square-tower fork addition (D3-u, M4-21 fix): records that `session_id` (one of possibly
    /// several sessions closed for `node_id`'s pending resync replacement) has actually
    /// terminated. Returns `Some(live_mode)` -- and removes the `pending_resync` entry -- only
    /// once every session named in that replacement has terminated; otherwise returns `None` and
    /// leaves the (now smaller) waiting set in place. Prevents re-`Initiate`-ing while a peer's
    /// *other* concurrent session (the D3-k dedupe comment on `Initiate` documents how a peer can
    /// legitimately end up with more than one) is still closing, which would otherwise make
    /// `Initiate`'s own "skip if another session for this node still runs" dedupe silently drop
    /// the peer forever (see `pending_resync`'s field doc comment).
    fn record_pending_resync_termination(
        state: &mut TopicManagerState<M>,
        node_id: NodeId,
        session_id: SyncSessionId,
    ) -> Option<bool> {
        let done = {
            let (_, waiting) = state.pending_resync.get_mut(&node_id)?;
            waiting.remove(&session_id);
            waiting.is_empty()
        };
        done.then(|| {
            state
                .pending_resync
                .remove(&node_id)
                .expect("just checked")
                .0
        })
    }

    /// square-tower fork addition (D3-s, M4-16 review): called once the old session of a pending
    /// resync replacement has actually terminated (gracefully or by failure -- `reason` is only
    /// for the log line). Only proceeds if `node_id` is still in `active_sync_set` (it may have
    /// left via gossip `NeighbourDown`/`EndSync` or an explicit `Close`/`CloseAll` in the window
    /// between the resync's `Close` and this termination -- those handlers also clear
    /// `pending_resync` directly, but this is a second, independent gate against the same race)
    /// and re-runs the identical `ConnectionAuthoriser` check the original `Resync`/`InitiateSync`
    /// request ran -- authorisation state can change in that same window, and that first check is
    /// stale by the time the fresh session would actually be created. No bypass: `Initiate` is
    /// only sent after a fresh, successful `can_connect_on_topic`.
    async fn resume_pending_resync(
        myself: &ActorRef<ToTopicManager<M::Message>>,
        state: &TopicManagerState<M>,
        node_id: NodeId,
        live_mode: bool,
        reason: &'static str,
    ) {
        let topic = state.topic;

        if !state.active_sync_set.contains(&node_id) {
            info!(
                remote_node_id = %node_id.fmt_short(),
                topic = %topic.fmt_short(),
                "drop pending resync: peer no longer in active sync set"
            );
            return;
        }

        if state
            .connection_authoriser
            .can_connect_on_topic(node_id, topic)
            .await
        {
            state
                .connection_authoriser
                .send_event(ConnectionAuthoriserEvent::TopicAllowed {
                    topic,
                    node: node_id,
                })
                .await;
        } else {
            let event = ConnectionAuthoriserEvent::TopicBlocked {
                topic,
                node: node_id,
            };
            warn!("{}", event);
            state.connection_authoriser.send_event(event).await;
            info!(
                remote_node_id = %node_id.fmt_short(),
                topic = %topic.fmt_short(),
                "drop pending resync: no longer authorised"
            );
            return;
        }

        info!(
            remote_node_id = %node_id.fmt_short(),
            topic = %topic.fmt_short(),
            %live_mode,
            "resync: previous session {reason}, starting fresh session"
        );
        let _ = myself.send_message(ToTopicManager::Initiate {
            node_id,
            topic,
            live_mode,
        });
    }

    /// Remove a session from all manager state mappings.
    fn drop_session(state: &mut TopicManagerState<M>, id: SyncSessionId) {
        state.session_topic_map.drop(id);
        state.node_session_map.iter_mut().for_each(|(_, sessions)| {
            sessions.remove(&id);
        });
    }
}
