// SPDX-License-Identifier: MIT OR Apache-2.0

//! Networking test for `NodeBuilder::connection_authoriser`: install a pre-configured
//! `ConnectionAuthoriser` before the endpoint starts accepting connections.

use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;

use p2panda::Credentials;
use p2panda::network::MdnsDiscoveryMode;
use p2panda_net::connection_authoriser::{
    ConnectionAuthoriser, ConnectionAuthoriserEvent, ConnectionRole,
};
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;

/// Binds an ephemeral UDP socket on localhost to reserve a free port, then immediately drops it
/// so the caller can pass the address to `bind_port_v4`/`bootstrap_addr`. There's a small,
/// unavoidable TOCTOU window between reservation and rebinding, but this is the standard pattern
/// for handing a soon-to-be-listening address to a peer ahead of time (the crate under test
/// provides no accessor for the endpoint's bound address after spawn).
fn reserve_localhost_addr() -> SocketAddr {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind ephemeral UDP port");
    socket.local_addr().expect("resolve bound local address")
}

/// Awaits authoriser events until `predicate` matches one, or the overall timeout elapses.
async fn wait_for_event<F>(
    events: &mut Receiver<ConnectionAuthoriserEvent>,
    timeout: Duration,
    mut predicate: F,
) -> ConnectionAuthoriserEvent
where
    F: FnMut(&ConnectionAuthoriserEvent) -> bool,
{
    tokio::time::timeout(timeout, async {
        loop {
            match events.recv().await {
                Ok(event) if predicate(&event) => return event,
                Ok(_) => continue,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => panic!("authoriser event channel closed unexpectedly"),
            }
        }
    })
    .await
    .expect("expected authoriser event before timeout")
}

/// A node built with a restrictive `ConnectionAuthoriser` that allows nobody rejects an inbound
/// connection from a peer that only knows about it via `bootstrap_addr` (mDNS disabled, no
/// relay — `bootstrap_addr` is used here purely as the connectivity mechanism under test
/// conditions; the authoriser under test is what's exercised).
#[tokio::test]
async fn connection_authoriser_blocks_unallowed_peer() {
    let addr_b = reserve_localhost_addr();

    let credentials_b = Credentials::generate();
    let node_id_b = credentials_b.verifying_key();

    let authoriser_b = ConnectionAuthoriser::new();
    authoriser_b.restrictive().await; // Deny by default; nobody is on the allow-list.
    let mut authoriser_b_events = authoriser_b.events().await;

    let _node_b = p2panda::builder()
        .credentials(credentials_b)
        .mdns_mode(MdnsDiscoveryMode::Disabled)
        .bind_ip_v4(Ipv4Addr::LOCALHOST)
        .bind_port_v4(addr_b.port())
        .connection_authoriser(authoriser_b)
        .spawn()
        .await
        .expect("node B spawns");

    let credentials_a = Credentials::generate();
    let node_id_a = credentials_a.verifying_key();

    let _node_a = p2panda::builder()
        .credentials(credentials_a)
        .mdns_mode(MdnsDiscoveryMode::Disabled)
        .bootstrap_addr(node_id_b, addr_b)
        .spawn()
        .await
        .expect("node A spawns");

    // A's discovery walker dials B; B's restrictive authoriser rejects the inbound handshake.
    let event = wait_for_event(&mut authoriser_b_events, Duration::from_secs(20), |event| {
        matches!(
            event,
            ConnectionAuthoriserEvent::Blocked {
                node,
                role: ConnectionRole::Acceptor,
            } if *node == node_id_a
        )
    })
    .await;
    assert_eq!(
        event,
        ConnectionAuthoriserEvent::Blocked {
            node: node_id_a,
            role: ConnectionRole::Acceptor,
        }
    );
}
