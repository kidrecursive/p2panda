// SPDX-License-Identifier: MIT OR Apache-2.0

//! M4-14 (fork item D3-r): an operation a node's own store already holds (first ingested via one
//! topic) must still be live-pushed to a DIFFERENT topic's live sessions once it is (re)imported
//! into that topic's own stream -- exactly the shape of `p2panda::spaces::repair`'s raw-op
//! republish (`import_local`, `p2panda/src/spaces/repair.rs`), which reports `AlreadyExists` for
//! an op ingested earlier via a sibling topic and (before this fix) never reached a peer whose
//! live session on the importing topic predates the republish.
//!
//! `p2panda::spaces::repair::repair_space`'s `import_local` is crate-private, so this test uses
//! the public equivalent, `StreamPublisher::import` (`Source::ExternalStream`) -- both sources hit
//! the exact same match arm in `p2panda::streams::stream::process_operation_in`
//! (`Source::ExternalStream { .. } | Source::LocalStore`), so this is a faithful stand-in.
//!
//! Uses `bootstrap_addr` with mDNS disabled (mirrors `fork_bootstrap_addr.rs`) rather than mDNS
//! discovery: mDNS-based node discovery hangs in this project's sandboxed CI/dev environment (see
//! the card's own note and `p2panda/tests/spaces.rs`'s observed hang here).

use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;

use futures_util::stream;
use p2panda::network::MdnsDiscoveryMode;
use p2panda::streams::StreamEvent;
use p2panda::{Credentials, Node};
use p2panda_core::Topic;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

/// Binds an ephemeral UDP socket on localhost to reserve a free port, then immediately drops it so
/// the caller can pass the address to `bind_port_v4`/`bootstrap_addr` (mirrors
/// `fork_bootstrap_addr.rs`'s `reserve_localhost_addr`).
fn reserve_localhost_addr() -> SocketAddr {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind ephemeral UDP port");
    socket.local_addr().expect("resolve bound local address")
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Msg(String);

#[tokio::test]
async fn republished_already_known_op_is_live_pushed_to_new_topics_sessions()
-> Result<(), Box<dyn std::error::Error>> {
    p2panda_core::test_utils::setup_logging();

    let addr_a = reserve_localhost_addr();

    let credentials_a = Credentials::generate();
    let node_id_a = credentials_a.verifying_key();

    let node_a: Node = p2panda::builder()
        .credentials(credentials_a)
        .mdns_mode(MdnsDiscoveryMode::Disabled)
        .bind_ip_v4(Ipv4Addr::LOCALHOST)
        .bind_port_v4(addr_a.port())
        .spawn()
        .await
        .expect("node A spawns");

    let node_b: Node = p2panda::builder()
        .mdns_mode(MdnsDiscoveryMode::Disabled)
        .bootstrap_addr(node_id_a, addr_a)
        .spawn()
        .await
        .expect("node B spawns");

    let topic_x = Topic::random();
    let topic_y = Topic::random();

    // Node A creates and publishes an operation on topic X -- it now exists in A's own store.
    let (tx_a_x, _rx_a_x) = node_a.stream::<Msg>(topic_x).await?;
    let published = tx_a_x.publish(Msg("hello".into())).await?.await?;
    let op = published.operation.clone();

    // Node A also opens topic Y (nothing published there yet).
    let (tx_a_y, _rx_a_y) = node_a.stream::<Msg>(topic_y).await?;

    // Node B subscribes to topic Y ONLY (never topic X, so it can never have received `op` any
    // other way) and we wait until its live sync session with node A is up before importing
    // anything -- the session must predate the op's association with topic Y, otherwise ordinary
    // catch-up sync (not the live push under test) would trivially deliver it.
    let (_tx_b_y, mut rx_b_y) = node_b.stream::<Msg>(topic_y).await?;
    let sync_started = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match rx_b_y.next().await.expect("stream ended unexpectedly") {
                StreamEvent::SyncStarted { remote_node_id, .. } if remote_node_id == node_id_a => {
                    return;
                }
                _ => continue,
            }
        }
    })
    .await;
    assert!(
        sync_started.is_ok(),
        "node B never established a live sync session with node A on topic Y"
    );

    // Node A imports the SAME operation -- already in its own store from topic X -- into topic
    // Y's stream. This is exactly the shape of a repair republish: an op the store already holds
    // (ingest will report `AlreadyExists`), newly associated with a topic it wasn't published on
    // before.
    tx_a_y.import(stream::once(async move { op })).await?;

    // Node B must receive it on topic Y promptly via the live push -- not after the (default 30s)
    // periodic resync, and not never.
    let received = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx_b_y.next().await.expect("stream ended unexpectedly") {
                StreamEvent::Processed { .. } => return,
                _ => continue,
            }
        }
    })
    .await;

    assert!(
        received.is_ok(),
        "node B never received the republished (already-known) operation on topic Y within 5s \
         of its live session -- an already-stored op imported into a new topic is not being \
         live-pushed to that topic's existing live sessions"
    );

    Ok(())
}
