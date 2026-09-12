// SPDX-License-Identifier: MIT OR Apache-2.0

use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use futures_channel::mpsc::{self, SendError};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use p2panda_core::Topic;
use p2panda_core::test_utils::setup_logging;
use p2panda_sync::traits::{Manager as SyncManagerTrait, Protocol};
use p2panda_sync::{FromSync, ToSync};
use ractor::thread_local::{ThreadLocalActor, ThreadLocalActorSpawner};
use ractor::{ActorRef, call};
use rand::random;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

use crate::NodeId;
use crate::address_book::AddressBook;
use crate::addrs::NodeInfo;
use crate::connection_authoriser::ConnectionAuthoriser;
use crate::gossip::Gossip;
use crate::iroh_endpoint::Endpoint;
use crate::sync::actors::{SyncManager, ToSyncManager};
use crate::sync::handle::SyncHandle;
use crate::test_utils::{ApplicationArguments, test_args_from_seed};

const TEST_PROTOCOL_ID: [u8; 32] = [101; 32];

struct FailingNode {
    args: ApplicationArguments,
    sync_ref: ActorRef<ToSyncManager<DummySyncMessage, DummySyncEvent>>,
    // square-tower fork addition (D3-s): kept around (not just handed to `SyncManager::spawn`) so
    // tests can flip it to `restrictive` after the node has already spawned and prove a resync
    // still respects it.
    connection_authoriser: ConnectionAuthoriser,
}

impl FailingNode {
    pub async fn spawn(
        seed: [u8; 32],
        node_infos: Vec<NodeInfo>,
        sync_args: FailingSyncArgs,
    ) -> Self {
        let args = test_args_from_seed(seed);

        let address_book = AddressBook::builder().spawn().await.unwrap();

        // Pre-populate the address book with known addresses.
        for info in node_infos {
            address_book.insert_node_info(info).await.unwrap();
        }

        let endpoint = Endpoint::builder(address_book.clone())
            .config(args.iroh_config.clone())
            .signing_key(args.signing_key.clone())
            .spawn()
            .await
            .unwrap();

        let gossip = Gossip::builder(address_book.clone(), endpoint.clone())
            .spawn()
            .await
            .unwrap();

        let thread_pool = ThreadLocalActorSpawner::new();
        let connection_authoriser = ConnectionAuthoriser::default();
        let (sync_ref, _) =
            SyncManager::<DummySyncManager<FailingSyncArgs, FailingSyncProtocol>>::spawn(
                None,
                (
                    TEST_PROTOCOL_ID.to_vec(),
                    sync_args,
                    endpoint,
                    gossip,
                    connection_authoriser.clone(),
                ),
                thread_pool,
            )
            .await
            .unwrap();

        Self {
            args,
            sync_ref,
            connection_authoriser,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.args.verifying_key
    }

    pub fn shutdown(&self) {
        self.sync_ref.stop(None);
    }
}

#[derive(Debug, Error)]
enum SyncError {
    #[error("unexpected sync failure")]
    UnexpectedFailure,
}

#[derive(Debug, Clone)]
enum SyncBehaviour {
    Panic,
    Error,
    Wait,
    /// square-tower fork addition (D3-k): ends the session with `Ok(())` right away, mirroring
    /// `TopicLogSync::run()` returning `Ok(())` after receiving (or sending) a `Close` message --
    /// the "graceful end, no error" case `topic_manager.rs`'s `handle_supervisor_evt` never
    /// retries (only `ActorFailed` does).
    Graceful,
    /// square-tower fork addition (M4-16 review): like `Graceful` (ends with `Ok(())` after the
    /// same 200ms), but `DummySyncManager::session` additionally emits `SyncFinished` right after
    /// `SessionCreated` -- unlike every other behaviour in this harness (which never make
    /// `Manager::is_catch_up_finished` true for a session at all), this lets a test observe
    /// `TopicManager::Resync`'s "replace a caught-up session" branch (and the deferred
    /// re-`Initiate` once that session's own natural, ~200ms termination fires) deterministically,
    /// without depending on real network timing.
    CaughtUpThenGraceful,
}

#[derive(Debug)]
struct FailingSyncProtocol {
    behaviour: SyncBehaviour,
}

impl Protocol for FailingSyncProtocol {
    type Output = ();
    type Error = SyncError;
    type Message = ();

    async fn run(
        self,
        sink: &mut (impl Sink<Self::Message, Error = impl std::fmt::Debug> + Unpin),
        stream: &mut (impl Stream<Item = Result<Self::Message, impl std::fmt::Debug>> + Unpin),
    ) -> Result<Self::Output, Self::Error> {
        // Send one message otherwise the accepting peer will not be able to accept the connection.
        let _ = sink.send(()).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        match self.behaviour {
            SyncBehaviour::Panic => panic!(),
            SyncBehaviour::Error => Err(SyncError::UnexpectedFailure),
            SyncBehaviour::Wait => {
                while stream.next().await.is_some() {}
                Err(SyncError::UnexpectedFailure)
            }
            SyncBehaviour::Graceful | SyncBehaviour::CaughtUpThenGraceful => Ok(()),
        }
    }
}

#[derive(Clone, Debug)]
struct FailingSyncArgs {
    pub event_tx: broadcast::Sender<FromSync<DummySyncEvent>>,
    pub behaviour: SyncBehaviour,
}

impl FailingSyncArgs {
    pub fn new(behaviour: SyncBehaviour) -> (Self, broadcast::Receiver<FromSync<DummySyncEvent>>) {
        let (tx, rx) = broadcast::channel(128);
        (
            Self {
                event_tx: tx,
                behaviour,
            },
            rx,
        )
    }
}

#[derive(Clone, Debug)]
#[allow(unused)]
enum DummySyncEvent {
    SessionCreated,
    SyncStarted,
    Received(DummySyncMessage),
    SyncFinished,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum DummySyncMessage {
    Data,
    Close,
}

#[derive(Debug)]
struct DummySyncManager<C, P> {
    pub event_tx: broadcast::Sender<FromSync<DummySyncEvent>>,
    #[allow(unused)]
    pub event_rx: broadcast::Receiver<FromSync<DummySyncEvent>>,
    pub args: C,
    pub _marker: PhantomData<P>,
}

impl SyncManagerTrait<Topic> for DummySyncManager<FailingSyncArgs, FailingSyncProtocol> {
    type Protocol = FailingSyncProtocol;
    type Event = DummySyncEvent;
    type Args = FailingSyncArgs;
    type Message = DummySyncMessage;
    type Error = SendError;

    fn from_args(args: Self::Args) -> Self {
        let event_rx = args.event_tx.subscribe();
        DummySyncManager {
            event_tx: args.event_tx.clone(),
            event_rx,
            args,
            _marker: PhantomData,
        }
    }

    async fn session(
        &mut self,
        session_id: u64,
        config: &p2panda_sync::SessionConfig<Topic>,
    ) -> Self::Protocol {
        self.event_tx
            .send(FromSync {
                session_id,
                remote: config.remote,
                event: DummySyncEvent::SessionCreated,
            })
            .unwrap();
        // square-tower fork addition (M4-16 review): `SyncBehaviour::CaughtUpThenGraceful` marks
        // this session as already caught up (see its own doc comment) the moment it's created --
        // no other behaviour in this harness ever does, so `Manager::is_catch_up_finished` stays
        // `false` for every session in every other test here (unaffected).
        if matches!(self.args.behaviour, SyncBehaviour::CaughtUpThenGraceful) {
            self.event_tx
                .send(FromSync {
                    session_id,
                    remote: config.remote,
                    event: DummySyncEvent::SyncFinished,
                })
                .unwrap();
        }
        FailingSyncProtocol {
            behaviour: self.args.behaviour.clone(),
        }
    }

    async fn session_handle(
        &self,
        _session_id: u64,
    ) -> Option<std::pin::Pin<Box<dyn Sink<ToSync<Self::Message>, Error = Self::Error>>>> {
        // Just a dummy channel to satisfy the API in testing environment.
        let (tx, _) = mpsc::channel::<ToSync<Self::Message>>(128);
        let sink = Box::pin(tx) as Pin<Box<dyn Sink<ToSync<Self::Message>, Error = Self::Error>>>;
        Some(sink)
    }

    fn subscribe(&mut self) -> impl Stream<Item = FromSync<Self::Event>> + Send + Unpin + 'static {
        let stream = BroadcastStream::new(self.event_tx.subscribe())
            .filter_map(|event| async { event.ok() });
        Box::pin(stream)
    }

    fn is_catch_up_finished(event: &Self::Event) -> bool {
        matches!(event, DummySyncEvent::SyncFinished)
    }
}

#[tokio::test]
async fn failed_sync_session_retry() {
    setup_logging();

    let topic = [0; 32].into();

    for (alice_behavior, bob_behavior) in [
        (SyncBehaviour::Panic, SyncBehaviour::Wait),
        (SyncBehaviour::Wait, SyncBehaviour::Panic),
        (SyncBehaviour::Error, SyncBehaviour::Wait),
        (SyncBehaviour::Wait, SyncBehaviour::Error),
        (SyncBehaviour::Error, SyncBehaviour::Error),
    ] {
        // Spawn nodes.
        let (bob_sync_config, _bob_rx) = FailingSyncArgs::new(bob_behavior);
        let mut bob = FailingNode::spawn(random(), vec![], bob_sync_config).await;

        let (alice_sync_config, _alice_rx) = FailingSyncArgs::new(alice_behavior);
        let alice =
            FailingNode::spawn(random(), vec![bob.args.node_info()], alice_sync_config).await;

        // Alice and Bob create stream for the same topic.
        let alice_handle = {
            let manager_ref = call!(alice.sync_ref, ToSyncManager::Create, topic, true).unwrap();
            SyncHandle::new(topic, alice.sync_ref.clone(), manager_ref)
        };
        let mut alice_subscription = alice_handle.subscribe().await.unwrap();

        let _bob_handle = {
            let manager_ref = call!(bob.sync_ref, ToSyncManager::Create, topic, true).unwrap();
            SyncHandle::new(topic, bob.sync_ref.clone(), manager_ref)
        };

        // Alice manually initiates a sync session with Bob.
        alice_handle.initiate_session(bob.node_id());

        let event = alice_subscription.next().await.unwrap();
        let expected_remote = bob.node_id();
        assert!(
            matches!(
                event,
                Ok(FromSync {
                    session_id: 0,
                    remote,
                    event: DummySyncEvent::SessionCreated
                }) if remote == expected_remote
            ),
            "{:#?}",
            event
        );
        let event = alice_subscription.next().await.unwrap();
        assert!(
            matches!(
                event,
                Ok(FromSync {
                    session_id: 1,
                    remote,
                    event: DummySyncEvent::SessionCreated
                }) if remote == expected_remote
            ),
            "{:#?}",
            event
        );

        alice.shutdown();
        bob.shutdown();
    }
}

/// square-tower fork addition (D3-k, `docs/upstream/p2panda-manual-resync.md`): a session that
/// ends *gracefully* (`Ok(())`, e.g. because gossip's HyParView active view dropped this peer and
/// `GossipEvent::NeighbourDown` triggered `ToSyncManager::EndSync` -> a normal `Close`) is never
/// automatically retried -- `handle_supervisor_evt`'s `ActorTerminated` arm only cleans up session
/// state, it never schedules `ToTopicManager::Retry` the way `ActorFailed` does. Proves both
/// halves: (1) the gap (no second `SessionCreated` shows up on its own) and (2) the fix (a manual
/// `SyncHandle::initiate_session` call -- now available outside test builds -- creates a fresh
/// session and recovers).
#[tokio::test]
async fn graceful_session_end_is_not_retried_but_manual_resync_recovers() {
    setup_logging();

    let topic = [1; 32].into();

    let (bob_sync_config, _bob_rx) = FailingSyncArgs::new(SyncBehaviour::Graceful);
    let mut bob = FailingNode::spawn(random(), vec![], bob_sync_config).await;

    let (alice_sync_config, _alice_rx) = FailingSyncArgs::new(SyncBehaviour::Graceful);
    let alice = FailingNode::spawn(random(), vec![bob.args.node_info()], alice_sync_config).await;

    let alice_handle = {
        let manager_ref = call!(alice.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, alice.sync_ref.clone(), manager_ref)
    };
    let mut alice_subscription = alice_handle.subscribe().await.unwrap();

    let _bob_handle = {
        let manager_ref = call!(bob.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, bob.sync_ref.clone(), manager_ref)
    };

    let expected_remote = bob.node_id();

    // First session, initiated manually (mirrors the node's very first join).
    alice_handle.initiate_session(expected_remote);
    let event = alice_subscription.next().await.unwrap();
    assert!(
        matches!(
            event,
            Ok(FromSync {
                session_id: 0,
                remote,
                event: DummySyncEvent::SessionCreated
            }) if remote == expected_remote
        ),
        "{:#?}",
        event
    );

    // The session ends gracefully (`SyncBehaviour::Graceful` returns `Ok(())`). Give it time to
    // actually terminate and confirm no second `SessionCreated` appears on its own within a
    // generous window -- this is the defect: `ActorTerminated` alone never retries.
    let no_auto_retry =
        tokio::time::timeout(Duration::from_millis(800), alice_subscription.next()).await;
    assert!(
        no_auto_retry.is_err(),
        "a gracefully-ended session must not be retried automatically, but got: {no_auto_retry:?}"
    );

    // The fix: a manual resync (what the node-side periodic resync task now calls on an
    // interval) creates a fresh session and recovers. Retried in a bounded loop rather than
    // called once: `Initiate` now dedupes against an already-running session for this peer+topic
    // (a *different* fork fix, also part of M4-07 -- `TopicManager::Initiate` skips spawning if
    // `node_session_map` still shows the just-ended session before its own `ActorTerminated`
    // cleanup has run), so a resync call that lands in that narrow cleanup window is a same-effect
    // no-op, not a failure -- exactly what a periodic caller (this test's stand-in for
    // `runtime::spawn_resync_task`) is expected to tolerate by simply trying again.
    let event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            alice_handle.initiate_session(expected_remote);
            match tokio::time::timeout(Duration::from_millis(200), alice_subscription.next()).await
            {
                Ok(event) => break event.unwrap(),
                Err(_elapsed) => continue,
            }
        }
    })
    .await
    .expect("manual resync should eventually start a fresh session");
    assert!(
        matches!(
            event,
            Ok(FromSync {
                session_id: 1,
                remote,
                event: DummySyncEvent::SessionCreated
            }) if remote == expected_remote
        ),
        "manual resync should start a fresh session: {:#?}",
        event
    );

    alice.shutdown();
    bob.shutdown();
}

/// square-tower fork addition (D3-s, `docs/upstream/p2panda-resync-replaces-session.md`): a
/// resync must not replace a session whose catch-up is still in progress -- `DummySyncManager`
/// (this file's own test harness) never emits `SyncFinished` at all, so any session built on it
/// is, by construction, permanently "still catching up" from `Manager::is_catch_up_finished`'s
/// point of view. This proves `SyncHandle::resync` skips such a session (no second
/// `SessionCreated`) rather than churning it, no matter how many times it's called.
#[tokio::test]
async fn resync_skips_while_catch_up_in_progress() {
    setup_logging();

    let topic = [2; 32].into();

    // `Wait` keeps the session open, blocked reading, indefinitely.
    let (bob_sync_config, _bob_rx) = FailingSyncArgs::new(SyncBehaviour::Wait);
    let mut bob = FailingNode::spawn(random(), vec![], bob_sync_config).await;

    let (alice_sync_config, _alice_rx) = FailingSyncArgs::new(SyncBehaviour::Wait);
    let alice = FailingNode::spawn(random(), vec![bob.args.node_info()], alice_sync_config).await;

    let alice_handle = {
        let manager_ref = call!(alice.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, alice.sync_ref.clone(), manager_ref)
    };
    let mut alice_subscription = alice_handle.subscribe().await.unwrap();

    let _bob_handle = {
        let manager_ref = call!(bob.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, bob.sync_ref.clone(), manager_ref)
    };

    let expected_remote = bob.node_id();

    alice_handle.initiate_session(expected_remote);
    let event = alice_subscription.next().await.unwrap();
    assert!(
        matches!(
            event,
            Ok(FromSync {
                session_id: 0,
                remote,
                event: DummySyncEvent::SessionCreated
            }) if remote == expected_remote
        ),
        "{:#?}",
        event
    );

    // Try to resync repeatedly while the session is still open (`Wait` never finishes catch-up).
    // None of these may create a second session.
    for _ in 0..5 {
        alice_handle.resync(expected_remote);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let no_replacement =
        tokio::time::timeout(Duration::from_millis(500), alice_subscription.next()).await;
    assert!(
        no_replacement.is_err(),
        "resync must not replace a session whose catch-up never finished, but got: {no_replacement:?}"
    );

    alice.shutdown();
    bob.shutdown();
}

/// square-tower fork addition (D3-s): a resync must run the exact same `ConnectionAuthoriser`
/// check `initiate_session` runs -- no bypass. Mirrors
/// `graceful_session_end_is_not_retried_but_manual_resync_recovers`'s scaffold (a `Graceful`
/// session ends on its own after ~200ms, so `node_session_map` is empty and a subsequent resync
/// takes the "no live session -> plain Initiate" branch) but flips the authoriser to
/// `restrictive` (no explicit allow for the remote) first: unlike that test, no fresh
/// `SessionCreated` may appear.
#[tokio::test]
async fn resync_respects_connection_authoriser_block() {
    setup_logging();

    let topic = [3; 32].into();

    let (bob_sync_config, _bob_rx) = FailingSyncArgs::new(SyncBehaviour::Graceful);
    let mut bob = FailingNode::spawn(random(), vec![], bob_sync_config).await;

    let (alice_sync_config, _alice_rx) = FailingSyncArgs::new(SyncBehaviour::Graceful);
    let alice = FailingNode::spawn(random(), vec![bob.args.node_info()], alice_sync_config).await;

    let alice_handle = {
        let manager_ref = call!(alice.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, alice.sync_ref.clone(), manager_ref)
    };
    let mut alice_subscription = alice_handle.subscribe().await.unwrap();

    let _bob_handle = {
        let manager_ref = call!(bob.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, bob.sync_ref.clone(), manager_ref)
    };

    let expected_remote = bob.node_id();

    // First session, allowed by the default-permissive authoriser.
    alice_handle.initiate_session(expected_remote);
    let event = alice_subscription.next().await.unwrap();
    assert!(
        matches!(
            event,
            Ok(FromSync {
                session_id: 0,
                remote,
                event: DummySyncEvent::SessionCreated
            }) if remote == expected_remote
        ),
        "{:#?}",
        event
    );

    // Let the graceful session actually terminate (~200ms sleep + Ok(())) so `node_session_map`
    // is empty by the time we call resync, exercising the "no live session" branch -- the same
    // branch `graceful_session_end_is_not_retried_but_manual_resync_recovers` proves creates a
    // fresh session when nothing blocks it.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Block: the identical call sequence must now produce no new session at all.
    alice.connection_authoriser.restrictive().await;

    alice_handle.resync(expected_remote);

    let blocked = tokio::time::timeout(Duration::from_millis(800), alice_subscription.next()).await;
    assert!(
        blocked.is_err(),
        "a resync must not bypass the ConnectionAuthoriser block, but got: {blocked:?}"
    );

    alice.shutdown();
    bob.shutdown();
}

/// square-tower fork addition (M4-16 review): positive control for `TopicManager::Resync`'s "no
/// live session" branch -- with the default-permissive authoriser and no prior session, `resync`
/// must create one, exactly like `initiate_session`. (This harness's `DummySyncManager` never
/// emits anything resembling the real protocol's `SyncStarted` -- the observable event here is
/// `SessionCreated`, the same one every other test in this file asserts on.)
#[tokio::test]
async fn resync_with_no_live_session_creates_one() {
    setup_logging();

    let topic = [4; 32].into();

    let (bob_sync_config, _bob_rx) = FailingSyncArgs::new(SyncBehaviour::Graceful);
    let mut bob = FailingNode::spawn(random(), vec![], bob_sync_config).await;

    let (alice_sync_config, _alice_rx) = FailingSyncArgs::new(SyncBehaviour::Graceful);
    let alice = FailingNode::spawn(random(), vec![bob.args.node_info()], alice_sync_config).await;

    let alice_handle = {
        let manager_ref = call!(alice.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, alice.sync_ref.clone(), manager_ref)
    };
    let mut alice_subscription = alice_handle.subscribe().await.unwrap();

    let _bob_handle = {
        let manager_ref = call!(bob.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, bob.sync_ref.clone(), manager_ref)
    };

    let expected_remote = bob.node_id();

    // No prior session at all -- the default-permissive authoriser must let this create one.
    alice_handle.resync(expected_remote);

    let event = tokio::time::timeout(Duration::from_secs(2), alice_subscription.next())
        .await
        .expect("resync with no live session should create one")
        .unwrap();
    assert!(
        matches!(
            event,
            Ok(FromSync {
                session_id: 0,
                remote,
                event: DummySyncEvent::SessionCreated
            }) if remote == expected_remote
        ),
        "{event:#?}"
    );

    alice.shutdown();
    bob.shutdown();
}

/// square-tower fork addition (M4-16 review): a resync replacement's deferred re-`Initiate` (fired
/// once the old, caught-up session actually terminates) must not resurrect a session for a peer
/// that left `active_sync_set` in the meantime (gossip `NeighbourDown` -> `EndSync`, simulated
/// directly here via `ToSyncManager::EndSync`). Mutation-proof: reverting the `active_sync_set`
/// gate in `topic_manager.rs`'s `resume_pending_resync` (and the `Close` handler's own
/// `pending_resync` removal) makes this test fail -- both runs captured in the PR.
#[tokio::test]
async fn close_during_pending_resync_does_not_resurrect_session() {
    setup_logging();

    let topic = [5; 32].into();

    let (bob_sync_config, _bob_rx) = FailingSyncArgs::new(SyncBehaviour::Graceful);
    let mut bob = FailingNode::spawn(random(), vec![], bob_sync_config).await;

    let (alice_sync_config, _alice_rx) = FailingSyncArgs::new(SyncBehaviour::CaughtUpThenGraceful);
    let alice = FailingNode::spawn(random(), vec![bob.args.node_info()], alice_sync_config).await;

    let alice_handle = {
        let manager_ref = call!(alice.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, alice.sync_ref.clone(), manager_ref)
    };
    let mut alice_subscription = alice_handle.subscribe().await.unwrap();

    let _bob_handle = {
        let manager_ref = call!(bob.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, bob.sync_ref.clone(), manager_ref)
    };

    let expected_remote = bob.node_id();

    // First session: `CaughtUpThenGraceful` marks it caught up immediately, then ends on its own
    // after ~200ms (matching `Graceful`'s timing) -- this harness's `ToSync::Close` is a no-op
    // (`DummySyncManager::session_handle`'s dummy channel has no live receiver), so a *natural*
    // end is what actually exercises "the old session terminates" here.
    alice_handle.initiate_session(expected_remote);
    let event = alice_subscription.next().await.unwrap();
    assert!(
        matches!(
            event,
            Ok(FromSync {
                session_id: 0,
                event: DummySyncEvent::SessionCreated,
                ..
            })
        ),
        "{event:#?}"
    );
    // `CaughtUpThenGraceful` also emits `SyncFinished` synchronously, right after
    // `SessionCreated`, onto this same subscription -- consume it explicitly so it isn't mistaken
    // for a second session's event later.
    let event = alice_subscription.next().await.unwrap();
    assert!(
        matches!(
            event,
            Ok(FromSync {
                session_id: 0,
                event: DummySyncEvent::SyncFinished,
                ..
            })
        ),
        "{event:#?}"
    );

    // Let `TopicManager`'s own background catch-up tracking (a separate task reading the same
    // broadcast channel) actually record this session's `SyncFinished` before resyncing.
    tokio::time::sleep(Duration::from_millis(50)).await;
    alice_handle.resync(expected_remote);

    // Simulate gossip's `NeighbourDown` -> `EndSync` for bob landing before the old session's own
    // ~200ms termination.
    alice
        .sync_ref
        .send_message(ToSyncManager::EndSync(topic, expected_remote))
        .unwrap();

    // Give the old session plenty of time to actually terminate and confirm no second session is
    // ever created.
    let no_resurrection =
        tokio::time::timeout(Duration::from_millis(800), alice_subscription.next()).await;
    assert!(
        no_resurrection.is_err(),
        "a resync replacement must not resurrect a session for a peer that left the active sync \
         set while the old session was closing, but got: {no_resurrection:?}"
    );

    alice.shutdown();
    bob.shutdown();
}

/// square-tower fork addition (M4-16 review): a resync replacement's deferred re-`Initiate` must
/// re-run `ConnectionAuthoriser::can_connect_on_topic` at the point it actually fires, not rely
/// only on the check that ran (successfully) before the old session was asked to close.
/// Mutation-proof: reverting `resume_pending_resync`'s authorisation re-check makes this test fail
/// (a second session appears) -- both runs captured in the PR.
#[tokio::test]
async fn deferred_resync_respects_authoriser() {
    setup_logging();

    let topic = [6; 32].into();

    let (bob_sync_config, _bob_rx) = FailingSyncArgs::new(SyncBehaviour::Graceful);
    let mut bob = FailingNode::spawn(random(), vec![], bob_sync_config).await;

    let (alice_sync_config, _alice_rx) = FailingSyncArgs::new(SyncBehaviour::CaughtUpThenGraceful);
    let alice = FailingNode::spawn(random(), vec![bob.args.node_info()], alice_sync_config).await;

    let alice_handle = {
        let manager_ref = call!(alice.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, alice.sync_ref.clone(), manager_ref)
    };
    let mut alice_subscription = alice_handle.subscribe().await.unwrap();

    let _bob_handle = {
        let manager_ref = call!(bob.sync_ref, ToSyncManager::Create, topic, true).unwrap();
        SyncHandle::new(topic, bob.sync_ref.clone(), manager_ref)
    };

    let expected_remote = bob.node_id();

    alice_handle.initiate_session(expected_remote);
    let event = alice_subscription.next().await.unwrap();
    assert!(
        matches!(
            event,
            Ok(FromSync {
                session_id: 0,
                event: DummySyncEvent::SessionCreated,
                ..
            })
        ),
        "{event:#?}"
    );
    // `CaughtUpThenGraceful` also emits `SyncFinished` synchronously, right after
    // `SessionCreated`, onto this same subscription -- consume it explicitly so it isn't mistaken
    // for a second session's event later.
    let event = alice_subscription.next().await.unwrap();
    assert!(
        matches!(
            event,
            Ok(FromSync {
                session_id: 0,
                event: DummySyncEvent::SyncFinished,
                ..
            })
        ),
        "{event:#?}"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    alice_handle.resync(expected_remote);

    // Let the resync request's own (first) authorisation check -- run inside `ToSyncManager::
    // Resync`'s handler, before `TopicManager::Resync` ever sets `pending_resync` -- actually
    // complete while the authoriser is still permissive; otherwise blocking immediately below
    // could race that first check instead of the deferred one this test targets.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Authorisation changes in the window between the resync request and the old session's actual
    // (~200ms) termination.
    alice.connection_authoriser.block(expected_remote).await;

    let no_session =
        tokio::time::timeout(Duration::from_millis(800), alice_subscription.next()).await;
    assert!(
        no_session.is_err(),
        "a deferred resync replacement must not bypass a ConnectionAuthoriser block that took \
         effect after the request but before the old session terminated, but got: {no_session:?}"
    );

    alice.shutdown();
    bob.shutdown();
}
