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

    // The periodic resync (what this test stands in for) replaces the live session.
    alice_handle.resync(bob.node_id());

    // Bob must see the new log's operation delivered by the fresh session's ordinary log-diff
    // catch-up.
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = bob_subscription.next().await.unwrap().unwrap();
            if let Event::OperationReceived { operation, .. } = event.event {
                if operation.hash == new_header.hash() {
                    return;
                }
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

    let (session, _events_rx, _live_tx) = peer.topic_sync_protocol(topic.clone(), true);

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
