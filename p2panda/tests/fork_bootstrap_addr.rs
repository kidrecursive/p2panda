// SPDX-License-Identifier: MIT OR Apache-2.0

//! Networking test for `NodeBuilder::bootstrap_addr`: relay-less, IP-only bootstrap addresses.

use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;

use p2panda::network::MdnsDiscoveryMode;
use p2panda::{Credentials, NodeId};
use p2panda_net::addrs::{NodeInfo as NetNodeInfo, TransportAddress, TransportInfo};
use p2panda_net::connection_authoriser::{
    ConnectionAuthoriser, ConnectionAuthoriserEvent, ConnectionRole,
};
use p2panda_store::address_book::AddressBookStore;
use p2panda_store::sqlite::SqliteStore;
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

/// Reads the `NodeInfo` a node's own address book holds for `node_id`, via the same `SqliteStore`
/// that backs the node (`Node::store()`, gated behind the `test_utils` feature).
async fn node_info_for(store: &SqliteStore, node_id: NodeId) -> Option<NetNodeInfo> {
    AddressBookStore::<NodeId, NetNodeInfo>::node_info(store, &node_id)
        .await
        .expect("address book query succeeds")
}

/// Two in-process nodes on `127.0.0.1` with random ports, mDNS disabled and no relay: B is given
/// A's address via `bootstrap_addr` only. B's address book must end up holding A with an IP-only
/// transport, and B's discovery walker must actually manage to connect to A (observed as an
/// `Allowed`, inbound event on A's own `ConnectionAuthoriser`).
#[tokio::test]
async fn bootstrap_addr_connects_without_relay() {
    let addr_a = reserve_localhost_addr();

    let credentials_a = Credentials::generate();
    let node_id_a = credentials_a.verifying_key();

    let authoriser_a = ConnectionAuthoriser::new();
    let mut authoriser_a_events = authoriser_a.events().await;

    let _node_a = p2panda::builder()
        .credentials(credentials_a)
        .mdns_mode(MdnsDiscoveryMode::Disabled)
        .bind_ip_v4(Ipv4Addr::LOCALHOST)
        .bind_port_v4(addr_a.port())
        .connection_authoriser(authoriser_a)
        .spawn()
        .await
        .expect("node A spawns");

    let node_b = p2panda::builder()
        .mdns_mode(MdnsDiscoveryMode::Disabled)
        .bootstrap_addr(node_id_a, addr_a)
        .spawn()
        .await
        .expect("node B spawns");
    let node_id_b = node_b.id();

    // B's address book carries a trusted, IP-only (no relay) entry for A.
    let node_info = node_info_for(&node_b.store(), node_id_a)
        .await
        .expect("A present in B's address book");
    assert!(node_info.bootstrap, "A must be marked as a bootstrap node");
    let transports = node_info.transports.expect("transport info present for A");
    let TransportInfo::Trusted(trusted) = transports else {
        panic!("expected locally-trusted transport info, got {transports:?}");
    };
    let TransportAddress::Iroh(endpoint_addr) = trusted
        .addresses
        .first()
        .expect("at least one transport address");
    assert!(
        endpoint_addr.ip_addrs().any(|ip| *ip == addr_a),
        "expected {addr_a} among {:?}",
        endpoint_addr.ip_addrs().collect::<Vec<_>>()
    );
    assert!(
        endpoint_addr.relay_urls().next().is_none(),
        "bootstrap_addr must never attach a relay URL"
    );

    // B's discovery walker dials A using only that address-book entry (mDNS is disabled and no
    // relay is configured); A's own authoriser observes the resulting inbound connection.
    let event = wait_for_event(&mut authoriser_a_events, Duration::from_secs(20), |event| {
        matches!(
            event,
            ConnectionAuthoriserEvent::Allowed {
                node,
                role: ConnectionRole::Acceptor,
            } if *node == node_id_b
        )
    })
    .await;
    assert_eq!(
        event,
        ConnectionAuthoriserEvent::Allowed {
            node: node_id_b,
            role: ConnectionRole::Acceptor,
        }
    );
}
