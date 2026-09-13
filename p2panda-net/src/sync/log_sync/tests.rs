// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::time::Duration;

use iroh::Endpoint;
use iroh::endpoint::{Connection, presets};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use p2panda_core::logs::Logs;
use p2panda_core::test_utils::setup_logging;
use p2panda_core::{Operation, Topic};
use p2panda_net::codec::{into_codec_sink, into_codec_stream};
use p2panda_sync::FromSync;
use p2panda_sync::protocols::TopicLogSyncEvent as Event;
use p2panda_sync::test_utils::{Peer, TestTopicSyncMessage};
use p2panda_sync::traits::Protocol;
use tokio_stream::StreamExt;

use crate::test_utils::TestNode;

#[tokio::test]
async fn e2e_log_sync() {
    setup_logging();

    let topic: Topic = [0; 32].into();
    let log_id = 0;

    let mut bob = TestNode::spawn([11; 32], None).await;
    let mut alice = TestNode::spawn([10; 32], Some(bob.node_info())).await;

    alice
        .client
        .create_operation(b"Hello from Alice", log_id)
        .await;
    alice
        .client
        .associate(&topic, &HashMap::from([(alice.client_id(), vec![log_id])]))
        .await;

    bob.client.create_operation(b"Hello from Bob", log_id).await;
    bob.client
        .associate(&topic, &HashMap::from([(bob.client_id(), vec![log_id])]))
        .await;

    let alice_handle = alice.log_sync.stream(topic, true).await.unwrap();
    let mut alice_subscription = alice_handle.subscribe().await.unwrap();

    let bob_handle = bob.log_sync.stream(topic, true).await.unwrap();
    let mut bob_subscription = bob_handle.subscribe().await.unwrap();

    alice_handle.initiate_session(bob.node_id());

    // Assert Alice receives the expected events.
    let bob_id = bob.node_id();
    let event = alice_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            session_id: 0,
            event: Event::LogsResolved { .. },
            ..
        })
    );
    let event = alice_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            session_id: 0,
            remote,
            event: Event::SyncStarted { .. },
        }) if remote == bob_id
    );
    let event = alice_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::OperationReceived { .. },
            ..
        })
    );
    let event = alice_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::SyncFinished { .. },
            ..
        })
    );
    let event = alice_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::LiveModeStarted,
            ..
        })
    );

    // Assert Bob receives the expected events.
    let alice_id = alice.node_id();
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            session_id: 0,
            event: Event::LogsResolved { .. },
            ..
        })
    );
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            session_id: 0,
            remote,
            event: Event::SyncStarted { .. },
        }) if remote == alice_id
    );
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::OperationReceived { .. },
            ..
        })
    );
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::SyncFinished { .. },
            ..
        })
    );
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::LiveModeStarted,
            ..
        })
    );

    // Alice publishes a live message.
    let (header, _, body) = alice
        .client
        .create_operation(b"live message from Alice", log_id)
        .await;
    alice_handle
        .publish(Operation {
            hash: header.hash(),
            header,
            body: Some(body),
        })
        .unwrap();

    // Bob receives Alice's live message.
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::OperationReceived { .. },
            ..
        })
    );

    // Drop Alice's stream to enforce closing live session with Bob.
    drop(alice_handle);

    // Both peers observe a clean session close.
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::SessionFinished { .. },
            ..
        })
    );
    let event = alice_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::SessionFinished { .. },
            ..
        })
    );
}

#[tokio::test]
async fn e2e_three_party_sync() {
    setup_logging();

    let topic: Topic = [0; 32].into();
    let log_id = 0;

    let mut bob = TestNode::spawn([30; 32], None).await;
    let mut alice = TestNode::spawn([31; 32], Some(bob.node_info())).await;
    let mut carol = TestNode::spawn([32; 32], Some(alice.node_info())).await;

    alice
        .client
        .create_operation(b"Hello from Alice", log_id)
        .await;
    alice
        .client
        .associate(&topic, &HashMap::from([(alice.client_id(), vec![log_id])]))
        .await;

    bob.client.create_operation(b"Hello from Bob", log_id).await;
    bob.client
        .associate(&topic, &HashMap::from([(bob.client_id(), vec![log_id])]))
        .await;

    carol
        .client
        .create_operation(b"Hello from Carol", log_id)
        .await;
    carol
        .client
        .associate(&topic, &HashMap::from([(carol.client_id(), vec![log_id])]))
        .await;

    let alice_handle = alice.log_sync.stream(topic, true).await.unwrap();
    let mut alice_subscription = alice_handle.subscribe().await.unwrap();

    let bob_handle = bob.log_sync.stream(topic, true).await.unwrap();
    let mut bob_subscription = bob_handle.subscribe().await.unwrap();

    alice_handle.initiate_session(bob.node_id());

    // Assert Alice receives the expected events.
    let bob_id = bob.node_id();
    let event = alice_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            session_id: 0,
            event: Event::LogsResolved { .. },
            ..
        })
    );
    let event = alice_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            session_id: 0,
            remote,
            event: Event::SyncStarted { .. },
        }) if remote == bob_id
    );
    let event = alice_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::OperationReceived { .. },
            ..
        })
    );
    let event = alice_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::SyncFinished { .. },
            ..
        })
    );
    let event = alice_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::LiveModeStarted,
            ..
        })
    );

    // Assert Bob receives the expected events.
    let alice_id = alice.node_id();
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            session_id: 0,
            event: Event::LogsResolved { .. },
            ..
        })
    );
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            session_id: 0,
            remote,
            event: Event::SyncStarted { .. },
        }) if remote == alice_id
    );
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::OperationReceived { .. },
            ..
        })
    );
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::SyncFinished { .. },
            ..
        })
    );
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::LiveModeStarted,
            ..
        })
    );

    // Alice publishes a live mode message.
    let (header, _, body) = alice
        .client
        .create_operation(b"live message from Alice", log_id)
        .await;
    alice_handle
        .publish(Operation {
            hash: header.hash(),
            header,
            body: Some(body),
        })
        .unwrap();

    // Bob receives Alice's live message.
    let event = bob_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::OperationReceived { .. },
            ..
        })
    );

    // Carol creates her stream and initiates sync with Alice.
    let carol_handle = carol.log_sync.stream(topic, true).await.unwrap();
    let mut carol_subscription = carol_handle.subscribe().await.unwrap();

    carol_handle.initiate_session(alice.node_id());

    let event = carol_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            session_id: 0,
            event: Event::LogsResolved { .. },
            ..
        })
    );
    let event = carol_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            session_id: 0,
            event: Event::SyncStarted { .. },
            ..
        })
    );
    let event = carol_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::OperationReceived { .. },
            ..
        })
    );
    let event = carol_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::OperationReceived { .. },
            ..
        })
    );
    let event = carol_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::SyncFinished { .. },
            ..
        })
    );
    let event = carol_subscription.next().await.unwrap();
    std::assert_matches!(
        event,
        Ok(FromSync {
            event: Event::LiveModeStarted,
            ..
        })
    );
}

/// square-tower fork addition (D3-s, `docs/upstream/p2panda-resync-replaces-session.md`): a
/// `TopicLogSync` session's offered log set is frozen once it resolves (D24-13) -- any
/// `(topic, author, log)` association made after that point is unreachable to that peer for the
/// rest of that session's lifetime, and the ordinary `Initiate` dedupe skips spawning a new
/// session while any session with that peer is still open. This proves the fix: after Alice's
/// live session with Bob has already resolved (both sides through `LiveModeStarted`), Alice
/// associates a brand new log with the topic and calls `SyncHandle::resync` -- Bob must receive
/// that new log's operation via the *fresh* session's ordinary log-diff catch-up (nothing is
/// published to the live channel), not merely "eventually". Fails before the fix (the new log is
/// never offered to the still-open, frozen session and nothing ever re-diffs it) -- mutation
/// evidence in the PR pastes both runs.
#[tokio::test]
async fn resync_replaces_live_session_recovering_late_association() {
    setup_logging();

    let topic: Topic = [90; 32].into();
    let existing_log_id = 0;
    let new_log_id = 1;

    let mut bob = TestNode::spawn([80; 32], None).await;
    let mut alice = TestNode::spawn([81; 32], Some(bob.node_info())).await;

    alice
        .client
        .create_operation(b"alice's first log", existing_log_id)
        .await;
    alice
        .client
        .associate(
            &topic,
            &HashMap::from([(alice.client_id(), vec![existing_log_id])]),
        )
        .await;

    bob.client
        .create_operation(b"bob's first log", existing_log_id)
        .await;
    bob.client
        .associate(
            &topic,
            &HashMap::from([(bob.client_id(), vec![existing_log_id])]),
        )
        .await;

    let alice_handle = alice.log_sync.stream(topic, true).await.unwrap();
    let mut alice_subscription = alice_handle.subscribe().await.unwrap();

    let bob_handle = bob.log_sync.stream(topic, true).await.unwrap();
    let mut bob_subscription = bob_handle.subscribe().await.unwrap();

    alice_handle.initiate_session(bob.node_id());

    // Drain both sides' first session (session_id 0) through to live mode -- the session is now
    // "resolved" in the D24-13 sense: its offered log set is frozen.
    for sub in [&mut alice_subscription, &mut bob_subscription] {
        loop {
            let event = tokio::time::timeout(Duration::from_secs(10), sub.next())
                .await
                .expect("first session should reach live mode")
                .unwrap()
                .unwrap();
            if matches!(event.event, Event::LiveModeStarted) {
                break;
            }
        }
    }

    // A NEW log is created and associated with the topic *after* the live session above already
    // resolved its offered log set.
    let (new_header, _, _) = alice
        .client
        .create_operation(b"alice's new log, associated after resolve", new_log_id)
        .await;
    alice
        .client
        .associate(
            &topic,
            &HashMap::from([(alice.client_id(), vec![new_log_id])]),
        )
        .await;

    // square-tower fork addition (D3-u, M4-21): no explicit `resync()` call -- the association
    // above pushes an `AssociationChanged` notification to alice's topic manager (via
    // `Manager::subscribe_new_associations`), which structurally compares the fresh resolved log
    // set against the live session's `LogsResolved` baseline and replaces it on its own, without
    // waiting for `sync.resync_interval`'s timer.

    // Bob must see the new log's operation delivered by the fresh session's ordinary log-diff
    // catch-up.
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = bob_subscription.next().await.unwrap().unwrap();
            if let Event::OperationReceived { operation, .. } = event.event
                && operation.hash == new_header.hash()
            {
                return;
            }
        }
    })
    .await;
    assert!(
        found.is_ok(),
        "bob must receive the late-associated log's operation after alice's resync"
    );
}

#[tokio::test]
async fn unsubscribe_from_gossip_after_drop() {
    setup_logging();

    let sync_topic: Topic = [0; 32].into();

    let alice = TestNode::spawn([73; 32], None).await;
    let alice_handle = alice.log_sync.stream(sync_topic, true).await.unwrap();

    let mut watcher = alice
        .address_book
        .watch_node_topics(alice.node_id(), false)
        .await
        .unwrap();

    while let Some(event) = watcher.recv().await {
        if !event.value.contains(&sync_topic) && event.value.len() == 1 {
            break;
        }
    }

    drop(alice_handle);

    while let Some(event) = watcher.recv().await {
        if event.value.is_empty() {
            break;
        }
    }
}

const ALPN: &[u8] = b"iroh/smol/0";

#[derive(Debug, Clone, Default)]
struct TestProtocol {}

impl ProtocolHandler for TestProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let _ = connection.accept_bi().await;
        // No need to do anything else here as we expect the connection to immediately close.
        Ok(())
    }

    async fn shutdown(&self) {}
}

#[tokio::test]
async fn panic_on_sink_closure_after_error_regression() {
    // This is a regression test for an issue where chaining adaptors on the message sink in
    // TopicLogSync was causing a panic under certain error conditions:
    // https://github.com/p2panda/p2panda/issues/970
    //
    // The issue could only be reproduced when using an actual QUIC stream as the underlying
    // transport. Here we use a connection between two iroh endpoints.
    setup_logging();

    let topic = Topic::random();
    let mut peer = Peer::new(0).await;
    peer.associate(&topic, &Logs::default()).await;

    let (session, _events_rx, _live_tx) = peer.topic_sync_protocol(topic, true);

    let acceptor = Endpoint::bind(presets::Minimal).await.unwrap();
    let acceptor_router = Router::builder(acceptor)
        .accept(ALPN, TestProtocol::default())
        .spawn();
    let addr = acceptor_router.endpoint().addr();

    let initiator = Endpoint::bind(presets::Minimal).await.unwrap();
    let connection = initiator.connect(addr, ALPN).await.unwrap();
    let (tx, rx) = connection.open_bi().await.unwrap();
    let mut tx = into_codec_sink::<TestTopicSyncMessage, _>(tx);
    let mut rx = into_codec_stream::<TestTopicSyncMessage, _>(rx);

    let handle = tokio::spawn(async move { session.run(&mut tx, &mut rx).await });

    // Unexpectedly closing the connection here on the "initiator" side causes the initial sync
    // protocol (before live-mode) to end with an error. After the error is correctly handled
    // sink.close() is called and _this_ causes a panic in the underlying message sink due to the
    // way it was wrapped in both a .with() and .sink_map_err() adaptor. The panic is caused
    // because both these wrappers end up calling poll_close() and doing this after the sink is
    // already in a closed state causes an error. The fix is to introduce a custom Sink wrapper
    // instead of chaining adaptors.
    connection.close(0u32.into(), b"testing");
    let result = handle.await.unwrap();
    assert!(result.is_err());
}

/// square-tower fork addition (D3-u, M4-21): a session whose own `LogsResolved` baseline already
/// matches the topic's current resolved set must never be replaced by `Resync`, even when another
/// peer's stale session (baseline captured before a later association) legitimately is -- the
/// structural (`!=`) comparison is per-session, not "replace everyone whenever anything changed".
///
/// Mutation-proof: dropping the `!=` filter in `TopicManager::handle`'s `Resync` arm (replacing
/// every current session unconditionally, as D3-s did) makes carol's explicit resync also produce
/// a new session, failing the final assertion.
#[tokio::test]
async fn association_change_replaces_only_stale_session() {
    setup_logging();

    let topic: Topic = [91; 32].into();
    let existing_log_id = 0;
    let new_log_id = 1;

    let mut bob = TestNode::spawn([82; 32], None).await;
    let mut alice = TestNode::spawn([83; 32], Some(bob.node_info())).await;
    let carol = TestNode::spawn([84; 32], Some(alice.node_info())).await;

    alice
        .client
        .create_operation(b"alice's first log", existing_log_id)
        .await;
    alice
        .client
        .associate(
            &topic,
            &HashMap::from([(alice.client_id(), vec![existing_log_id])]),
        )
        .await;

    let alice_handle = alice.log_sync.stream(topic, true).await.unwrap();
    let mut alice_subscription = alice_handle.subscribe().await.unwrap();

    let bob_handle = bob.log_sync.stream(topic, true).await.unwrap();
    let _bob_subscription = bob_handle.subscribe().await.unwrap();

    alice_handle.initiate_session(bob.node_id());

    // Drain alice's session with bob through to live mode -- its `LogsResolved` baseline is now
    // frozen at `{existing_log_id}`.
    let bob_session_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = alice_subscription.next().await.unwrap().unwrap();
            if event.session_id == 0 && matches!(event.event, Event::LiveModeStarted) {
                return event.session_id;
            }
        }
    })
    .await
    .expect("bob's first session should reach live mode");

    // A NEW log is associated *after* bob's session resolved -- this must replace his now-stale
    // session automatically (event-driven resync, no explicit `resync()` call).
    alice
        .client
        .create_operation(b"alice's new log", new_log_id)
        .await;
    alice
        .client
        .associate(
            &topic,
            &HashMap::from([(alice.client_id(), vec![new_log_id])]),
        )
        .await;

    let new_bob_session_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = alice_subscription.next().await.unwrap().unwrap();
            if event.session_id != bob_session_id
                && matches!(event.event, Event::SyncStarted { .. })
            {
                return event.session_id;
            }
        }
    })
    .await
    .expect("bob's stale session must be replaced with a fresh one");
    assert_ne!(new_bob_session_id, bob_session_id);

    // Carol connects *after* the new log was already associated -- her session's own
    // `LogsResolved` baseline already includes it, so she must never be treated as stale.
    let carol_handle = carol.log_sync.stream(topic, true).await.unwrap();
    let mut carol_subscription = carol_handle.subscribe().await.unwrap();
    carol_handle.initiate_session(alice.node_id());

    let carol_session_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = carol_subscription.next().await.unwrap().unwrap();
            if matches!(event.event, Event::LiveModeStarted) {
                return event.session_id;
            }
        }
    })
    .await
    .expect("carol's session should reach live mode");

    // An explicit resync (what the periodic timer sends every `sync.resync_interval`) must be a
    // no-op for carol's already-fresh session.
    alice_handle.resync(carol.node_id());
    let no_replacement = tokio::time::timeout(Duration::from_millis(800), async {
        loop {
            let event = carol_subscription.next().await.unwrap().unwrap();
            if event.session_id != carol_session_id {
                return event.session_id;
            }
        }
    })
    .await;
    assert!(
        no_replacement.is_err(),
        "carol's already-fresh session must not be replaced, but got: {no_replacement:?}"
    );
}

/// square-tower fork addition (D3-u, M4-21): calling `Resync` directly (what the periodic
/// `sync.resync_interval` timer does) with no association change since the session's own
/// `LogsResolved` baseline was captured must be a no-op -- the timer becomes a redundant fallback
/// once nothing has actually changed.
///
/// Mutation-proof: dropping the `!=` filter in `TopicManager::handle`'s `Resync` arm (replacing
/// the session unconditionally) makes the final assertion fail (a new session appears).
#[tokio::test]
async fn resync_timer_noop_when_unchanged() {
    setup_logging();

    let topic: Topic = [92; 32].into();
    let log_id = 0;

    let mut bob = TestNode::spawn([85; 32], None).await;
    let mut alice = TestNode::spawn([86; 32], Some(bob.node_info())).await;

    alice.client.create_operation(b"alice's log", log_id).await;
    alice
        .client
        .associate(&topic, &HashMap::from([(alice.client_id(), vec![log_id])]))
        .await;

    let alice_handle = alice.log_sync.stream(topic, true).await.unwrap();
    let mut alice_subscription = alice_handle.subscribe().await.unwrap();

    let bob_handle = bob.log_sync.stream(topic, true).await.unwrap();
    let _bob_subscription = bob_handle.subscribe().await.unwrap();

    alice_handle.initiate_session(bob.node_id());

    let session_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = alice_subscription.next().await.unwrap().unwrap();
            if matches!(event.event, Event::LiveModeStarted) {
                return event.session_id;
            }
        }
    })
    .await
    .expect("session should reach live mode");

    // No association changed since the session's own baseline was captured.
    alice_handle.resync(bob.node_id());
    let no_replacement = tokio::time::timeout(Duration::from_millis(800), async {
        loop {
            let event = alice_subscription.next().await.unwrap().unwrap();
            if event.session_id != session_id {
                return event.session_id;
            }
        }
    })
    .await;
    assert!(
        no_replacement.is_err(),
        "an unchanged log set must not be replaced, but got: {no_replacement:?}"
    );
}
