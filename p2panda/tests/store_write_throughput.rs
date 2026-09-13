// SPDX-License-Identifier: MIT OR Apache-2.0

//! M4-24: control's ingest throughput on a slow disk fell below the drones' publish rate after
//! the WAL/`BEGIN IMMEDIATE` store changes (M4-21/M4-22) -- every processed op cost three
//! serialized `BEGIN IMMEDIATE` write transactions (ingest, orderer, ack), each fsyncing the WAL
//! once under SQLite's default `synchronous=FULL`. This test exercises the real
//! publish -> ingest -> orderer -> ack pipeline (a single node, file-backed temp database, one
//! author) end to end and asserts a throughput floor.
//!
//! mDNS discovery is disabled (mirrors `live_push_republish.rs`/`create_space_sibling_logs.rs`):
//! it hangs in this project's sandboxed CI/dev environment, and this test doesn't need any peer
//! discovery -- it publishes to and reads from its own single node.

use std::time::Instant;

use p2panda::network::MdnsDiscoveryMode;
use p2panda::streams::StreamEvent;
use p2panda::{Credentials, Topic};
use tokio_stream::StreamExt;

/// Number of ops published (and processed through the full pipeline) by the throughput test.
const OP_COUNT: usize = 500;

/// Calibrated floor: on the test machine's disk, ingest through the fixed pipeline (WAL,
/// `synchronous=NORMAL`, batched acks) comfortably exceeds this. See the card's write-up
/// (`docs/upstream/p2panda-store-write-throughput.md`) for the before/after numbers this bar was
/// calibrated against.
const MIN_OPS_PER_SEC: f64 = 40.0;

fn unique_sqlite_url(label: &str) -> (std::path::PathBuf, String) {
    let path = std::env::temp_dir().join(format!(
        "p2panda-m4-24-{label}-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    (path, url)
}

#[tokio::test]
async fn ingest_orderer_ack_pipeline_throughput() {
    let (db_path, db_url) = unique_sqlite_url("throughput");

    let node = p2panda::builder()
        .database_url(&db_url)
        .credentials(Credentials::generate())
        .mdns_mode(MdnsDiscoveryMode::Disabled)
        .spawn()
        .await
        .expect("node spawns against a file-backed temp database");

    let topic = Topic::random();
    let (publisher, mut subscriber) = node
        .stream::<Vec<u8>>(topic)
        .await
        .expect("topic stream opens");

    let start = Instant::now();

    // Publish all ops from the single author up-front; the pipeline (ingest -> orderer -> ack)
    // processes them concurrently with publishing, exactly as a live drone's stream does.
    for i in 0..OP_COUNT {
        publisher
            .publish((i as u32).to_le_bytes().to_vec())
            .await
            .expect("publish enqueues into the processing pipeline");
    }

    let mut processed = 0usize;
    while processed < OP_COUNT {
        match subscriber.next().await.expect("subscription stays open") {
            StreamEvent::Processed { .. } => processed += 1,
            StreamEvent::AckFailed { error, .. } => {
                panic!("ack failed during throughput run: {error}")
            }
            StreamEvent::ProcessingFailed { error, .. } => {
                panic!("processing failed during throughput run: {error}")
            }
            // Any other system-level event (e.g. replay bookkeeping) is not a processed op.
            _ => {}
        }
    }

    let elapsed = start.elapsed();
    let ops_per_sec = OP_COUNT as f64 / elapsed.as_secs_f64();

    eprintln!(
        "M4-24 throughput: {OP_COUNT} ops in {:?} ({:.1} ops/s)",
        elapsed, ops_per_sec
    );

    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));

    assert!(
        ops_per_sec >= MIN_OPS_PER_SEC,
        "ingest throughput {ops_per_sec:.1} ops/s fell below the {MIN_OPS_PER_SEC} ops/s floor \
         calibrated for this pipeline (WAL, synchronous=NORMAL, batched acks) -- see \
         docs/upstream/p2panda-store-write-throughput.md",
    );
}
