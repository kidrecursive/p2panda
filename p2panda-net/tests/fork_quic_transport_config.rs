// SPDX-License-Identifier: MIT OR Apache-2.0

//! Networking test for M3-01 item (b), `IrohConfig::quic_transport_config` /
//! `iroh_endpoint::Builder::quic_transport_config`: the endpoint-wide default QUIC transport
//! parameters (previously hard-coded to 5s keep-alive / 10s max idle timeout) are now
//! caller-configurable.
//!
//! Builds node A with a 3s `max_idle_timeout`, connects it to node B (default config), then
//! drops B's endpoint entirely (every handle referencing its actor) to simulate B disappearing.
//! Asserts A observes the resulting disconnection within 5s.
//!
//! Connection-observer path used: `iroh::endpoint::Connection::closed()` -- the standard QUIC
//! "await until this connection is no longer usable" future, obtained directly from
//! `p2panda_net::Endpoint::connect()`'s return value. No `Endpoint::connections()`/equivalent
//! event stream exists on `p2panda_net::Endpoint` (checked); `Connection::closed()` is the
//! documented per-connection observer iroh itself provides.

use std::time::Duration;

use iroh::endpoint::{Connection, QuicTransportConfig};
use iroh::protocol::{AcceptError, ProtocolHandler};
use p2panda_core::test_utils::setup_logging;
use p2panda_net::test_utils::{TestNode, test_args_from_seed};
use tokio::time::timeout;

const ALPN: &[u8] = b"square-tower/fork-quic-transport-config-test/0";

/// Accepts a connection and holds it open (awaiting its own closure) so it survives for as long
/// as node B does -- per iroh's own `ProtocolHandler::accept` docs, the connection is otherwise
/// dropped (and closed) as soon as `accept()` returns.
#[derive(Debug, Clone)]
struct HoldOpen;

impl ProtocolHandler for HoldOpen {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        connection.closed().await;
        Ok(())
    }
}

#[tokio::test]
async fn quic_transport_config_governs_idle_disconnect() {
    setup_logging();

    // Node B: default transport config (the pre-existing 5s/10s hard-coded default).
    let mut node_b = TestNode::spawn([201; 32], None).await;
    node_b
        .endpoint
        .accept(ALPN, HoldOpen)
        .await
        .expect("B registers protocol handler");

    // Node A: 3s max idle timeout via the new `quic_transport_config` knob (replaces the 10s
    // default this card's item (b) makes overridable).
    let quic_transport_config = QuicTransportConfig::builder()
        .max_idle_timeout(Some(
            Duration::from_secs(3)
                .try_into()
                .expect("valid idle timeout"),
        ))
        .build();
    let mut args_a = test_args_from_seed([202; 32]);
    args_a.iroh_config.quic_transport_config = Some(quic_transport_config);
    let node_a = TestNode::spawn_with_args(args_a, Some(node_b.node_info().bootstrap())).await;

    // A dials B directly (address seeded via `node_info`, no discovery-walk dependency) using
    // the endpoint-wide default -- i.e. exactly the config path this card's item (b) threads
    // `quic_transport_config` through.
    let connection = node_a
        .endpoint
        .connect(node_b.node_id(), ALPN)
        .await
        .expect("A connects to B");
    assert!(
        connection.close_reason().is_none(),
        "connection must be alive immediately after connecting"
    );

    // B "stops abruptly": drop every handle referencing its endpoint actor (TestNode's mdns,
    // discovery, gossip, log_sync and endpoint fields all hold a clone of the same underlying
    // actor reference; dropping the whole TestNode drops the last one).
    drop(node_b);

    // A must observe the disconnection within 5s (a bound looser than the 3s idle timeout we
    // configured for A, giving headroom for the actual teardown to propagate).
    timeout(Duration::from_secs(5), connection.closed())
        .await
        .expect("A observes B's disconnection within 5s");
}
