// SPDX-License-Identifier: MIT OR Apache-2.0

use std::net::{Ipv4Addr, Ipv6Addr};

use iroh::endpoint::QuicTransportConfig;

#[derive(Clone, Debug)]
pub struct IrohConfig {
    /// IPv4 address to bind to.
    pub bind_ip_v4: Ipv4Addr,

    /// Port used for IPv4 socket address.
    ///
    /// Setting the port to `0` will use a random port. If the port specified is already in use, it
    /// will fallback to choosing a random port.
    pub bind_port_v4: u16,

    /// IPv6 address to bind to.
    pub bind_ip_v6: Ipv6Addr,

    /// Port used for IPv6 socket address.
    ///
    /// Setting the port to `0` will use a random port. If the port specified is already in use, it
    /// will fallback to choosing a random port.
    pub bind_port_v6: u16,

    /// Default QUIC transport parameters applied to the endpoint at bind time.
    ///
    /// When `None`, the endpoint falls back to the existing hard-coded default (5s keep-alive
    /// interval, 10s max idle timeout). This is the endpoint-wide default only: `LogSync`'s own
    /// sync sessions always dial with plain `connect()`, never `connect_with_config()`, so this is
    /// also the only transport-config knob that actually governs drone/control sync traffic.
    pub quic_transport_config: Option<QuicTransportConfig>,
}

impl Default for IrohConfig {
    fn default() -> Self {
        Self {
            bind_ip_v4: Ipv4Addr::UNSPECIFIED,
            bind_port_v4: 0,
            bind_ip_v6: Ipv6Addr::UNSPECIFIED,
            bind_port_v6: 0,
            quic_transport_config: None,
        }
    }
}
