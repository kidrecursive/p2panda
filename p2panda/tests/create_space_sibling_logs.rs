// SPDX-License-Identifier: MIT OR Apache-2.0

//! M4-14 round 2 (fork item D3-r): `Node::create_space` must associate every already-known sibling
//! group's log with the new space's topic BEFORE that topic's stream opens, not only after the new
//! space's own control messages persist. A peer whose live session on that topic resolves in the
//! window between "stream opens" and "sibling logs associated" gets an offered log set frozen
//! without those sibling logs (D24-13) -- and since `RepairStrategy::Global` (the only strategy in
//! use, `p2panda/src/spaces/repair.rs`) makes every space depend on every known group, this is not
//! a corner case: it is the default outcome whenever a space is created after other groups already
//! exist and a peer is already dialing in.
//!
//! Root cause (corrected mechanism, M4-14 round 2, refuting this card's original stage-1
//! hypothesis about the live push itself): a repair-republished op for a sibling group whose log
//! was never associated with the importing topic in time still arrives at a live peer (the live
//! push is unconditional, see `live_push_republish.rs`) -- but with no known predecessor for that
//! (author, log_id) at all, it goes into the ingest out-of-order buffer forever (D3-j), silently:
//! see `p2panda::stream::ooo_park` in `p2panda-stream`'s `Ingest` processor.
//!
//! Uses `bootstrap_addr` with mDNS disabled (mirrors `fork_bootstrap_addr.rs`/
//! `live_push_republish.rs`): mDNS-based node discovery hangs in this project's sandboxed
//! environment.

#[cfg(feature = "test-hooks")]
use std::io::Write;
#[cfg(feature = "test-hooks")]
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
#[cfg(feature = "test-hooks")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "test-hooks")]
use p2panda::network::MdnsDiscoveryMode;
#[cfg(feature = "test-hooks")]
use p2panda::{Credentials, Node};
#[cfg(feature = "test-hooks")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "test-hooks")]
use tracing_subscriber::fmt::MakeWriter;

/// Binds an ephemeral UDP socket on localhost to reserve a free port, then immediately drops it so
/// the caller can pass the address to `bind_port_v4`/`bootstrap_addr` (mirrors
/// `fork_bootstrap_addr.rs`'s `reserve_localhost_addr`).
#[cfg(feature = "test-hooks")]
fn reserve_localhost_addr() -> SocketAddr {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind ephemeral UDP port");
    socket.local_addr().expect("resolve bound local address")
}

#[cfg(feature = "test-hooks")]
async fn spawn_node_with_bootstrap(bootstrap: Option<(p2panda::NodeId, SocketAddr)>) -> Node {
    let mut builder = p2panda::builder().mdns_mode(MdnsDiscoveryMode::Disabled);
    if let Some((node_id, addr)) = bootstrap {
        builder = builder.bootstrap_addr(node_id, addr);
    }
    builder.spawn().await.expect("node spawns")
}

#[cfg(feature = "test-hooks")]
async fn spawn_bootstrap_node(addr: SocketAddr) -> Node {
    let credentials = Credentials::generate();
    p2panda::builder()
        .credentials(credentials)
        .mdns_mode(MdnsDiscoveryMode::Disabled)
        .bind_ip_v4(Ipv4Addr::LOCALHOST)
        .bind_port_v4(addr.port())
        .spawn()
        .await
        .expect("node spawns")
}

/// Writes captured tracing output into a shared in-memory buffer instead of stderr, so the test
/// can assert on log content directly (`ooo_park`'s absence/presence) rather than parsing files.
#[cfg(feature = "test-hooks")]
#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

#[cfg(feature = "test-hooks")]
impl Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if std::env::var("SQT_DEBUG_TEE").is_ok() {
            let _ = std::io::stderr().write_all(buf);
        }
        self.0
            .lock()
            .expect("capture buffer mutex poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "test-hooks")]
impl<'a> MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[cfg(feature = "test-hooks")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Empty;

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn sibling_group_log_reaches_early_peer_before_repair_is_ever_needed()
-> Result<(), Box<dyn std::error::Error>> {
    let buffer: Arc<Mutex<Vec<u8>>> = Arc::default();
    let writer = CaptureWriter(buffer.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_writer(writer)
        .with_ansi(false)
        .finish();
    let _tracing_guard = tracing::subscriber::set_default(subscriber);

    // Node A is control: owns space A (and later space C) and dials out to nobody -- everyone
    // else bootstraps directly to it (mDNS is disabled).
    let addr_a = reserve_localhost_addr();
    let node_a = spawn_bootstrap_node(addr_a).await;
    let node_id_a = node_a.id();

    // X and Y are *bare* auth actors -- raw public keys, no spawned node/network identity of
    // their own needed. `Group::add` (unlike `Space::add`) only needs an `ActorId`, no key-bundle
    // registration, so this sidesteps the encryption/DCGKA machinery entirely and keeps this test
    // scoped to what it's actually about: log association ordering, not encrypted membership.
    let node_id_x = Credentials::generate().verifying_key();
    let node_id_y = Credentials::generate().verifying_key();

    // Node B: the peer under test. Subscribes to topic C *before* control ever creates space C,
    // and must end up with group A's logs (associated before B's session resolves) despite never
    // subscribing to group A itself.
    let node_b = spawn_node_with_bootstrap(Some((node_id_a, addr_a))).await;
    let mut node_b_events = node_b.event_stream().await?;

    let space_c_id = Topic::random();

    // --- Set up group A (a bare auth group, no space) with member X -------------------------

    let group_a = node_a
        .create_group(&[(node_id_a, AccessLevel::Manage)])
        .await?;
    let group_a_id = group_a.id();
    group_a.add(node_id_x, AccessLevel::Read).await?;

    // --- Node B subscribes to topic C first, control creates space C second ----------------

    let (_tx_b_c, mut rx_b_c) = node_b.space::<Empty>(space_c_id).await?;

    // Arm the test hook: `create_space(C)`'s topic stream will open, then pause right there
    // (before space C's own control messages are forged/persisted) until we release it below.
    let pause = node_a.test_pause_next_create_space_after_stream_open();

    let create_space_c = node_a.create_space::<Empty>(space_c_id);
    tokio::pin!(create_space_c);

    // Drive `create_space_c` up to (and including) its pause point while concurrently waiting for
    // B's live session with A on topic C to start -- this is the exact window under test: B's
    // offered log set is frozen the moment its session resolves.
    loop {
        tokio::select! {
            biased;

            _result = &mut create_space_c => {
                panic!("create_space(C) returned before we could release its test-hook pause");
            }
            event = rx_b_c.next() => {
                match event.expect("space C stream ended on B") {
                    StreamEvent::SyncStarted { remote_node_id, .. } if remote_node_id == node_id_a => {
                        break;
                    }
                    _ => continue,
                }
            }
        }
    }
    pause.notify_one();

    let (_space_c, mut _rx_c) = create_space_c.await?;

    // Wait for B's first sync round with A on topic C to finish -- the round whose offered log
    // set was resolved during the pause above.
    let sync_ended = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match rx_b_c.next().await.expect("space C stream ended on B") {
                StreamEvent::SyncEnded { remote_node_id, .. } if remote_node_id == node_id_a => {
                    return;
                }
                _ => continue,
            }
        }
    })
    .await;
    assert!(
        sync_ended.is_ok(),
        "B's first sync round with A on topic C never ended"
    );

    // Assert B already holds group A's Create *and* the X `Add` -- delivered via that very first
    // sync round, proving group A's log was associated with topic C's own log set before B's
    // session resolved (not merely "eventually, via repair" -- the whole point of the fix).
    let mut saw_group_a_created = false;
    let mut saw_x_added = false;
    let group_a_ready = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match node_b_events.next().await.expect("event stream ended on B") {
                SystemEvent::Groups {
                    group_id, inner, ..
                } if group_id == group_a_id => match inner {
                    InnerGroupEvent::Created { .. } => saw_group_a_created = true,
                    InnerGroupEvent::Added { added, .. } if added.id() == node_id_x => {
                        saw_x_added = true;
                    }
                    _ => {}
                },
                _ => continue,
            }
            if saw_group_a_created && saw_x_added {
                return;
            }
        }
    })
    .await;
    assert!(
        group_a_ready.is_ok() && saw_group_a_created && saw_x_added,
        "B never observed group A's Create+Add (created={saw_group_a_created} added={saw_x_added}) \
         -- group A's log was not associated with topic C before B's session resolved"
    );

    // --- Now exercise the live-push/repair path: a NEW group A op, after space C exists -----

    group_a.add(node_id_y, AccessLevel::Read).await?;

    let saw_y_added = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match node_b_events.next().await.expect("event stream ended on B") {
                SystemEvent::Groups {
                    group_id,
                    inner: InnerGroupEvent::Added { added, .. },
                    ..
                } if group_id == group_a_id && added.id() == node_id_y => return,
                _ => continue,
            }
        }
    })
    .await;
    assert!(
        saw_y_added.is_ok(),
        "B never observed Y's Add to group A -- the repair-republished op parked forever \
         instead of arriving via the (already-established) live push"
    );

    // The definitive check: no operation on B ever parked with "no known predecessor at all" for
    // its (author, log_id) -- the previously-invisible resting place this card's round 2 added a
    // probe for. If group A's log was associated with topic C before B's session resolved (the
    // fix), every op B receives for it always has a known frontier by the time it arrives.
    let captured = String::from_utf8(
        buffer
            .lock()
            .expect("capture buffer mutex poisoned")
            .clone(),
    )
    .expect("captured tracing output is valid utf-8");
    let park_lines: Vec<&str> = captured
        .lines()
        .filter(|line| line.contains("ooo_park"))
        .collect();
    assert!(
        park_lines.is_empty(),
        "expected zero ooo_park lines, found {}:\n{}",
        park_lines.len(),
        park_lines.join("\n")
    );

    Ok(())
}
