// SPDX-License-Identifier: MIT OR Apache-2.0

//! Test for M3-01 item (c), `NodeBuilder::spaces_config`: replaces the previously hard-coded
//! `SpacesConfig::default()` (90 day pre-key lifetime / 60 day rotate-after) at `node.rs:114-116`
//! with a caller-supplied config.
//!
//! Builds a node with `pre_key_lifetime = 1h`, then checks that its own long-term key bundle
//! (via the `SpacesManager::me()` test accessor) carries a `Lifetime` matching that 1h window --
//! not the 90-day default. `Lifetime` has no public getter for its raw `not_after` timestamp, so
//! this uses `Lifetime::verify_with_window`, which is public API: with a 1h `pre_key_lifetime`, a
//! window of 3599s must still be valid (not yet expired) while a window of 3601s must not (already
//! past `not_after`) -- a default 90-day lifetime would trivially satisfy both windows, so this
//! distinguishes the configured value from the default.

use std::time::Duration;

use p2panda::SpacesConfig;
use p2panda_encryption::traits::KeyBundle;

#[tokio::test]
async fn spaces_config_pre_key_lifetime_is_applied() {
    let spaces_config = SpacesConfig {
        pre_key_lifetime: Duration::from_secs(60 * 60), // 1h
        pre_key_rotate_after: Duration::from_secs(30 * 60), // 30min
    };

    let node = p2panda::builder()
        .spaces_config(spaces_config)
        .spawn()
        .await
        .expect("node spawns");

    let member = node
        .spaces_manager()
        .me()
        .await
        .expect("own member/key bundle available");
    let lifetime = *member.key_bundle().lifetime();

    assert!(
        lifetime.verify_with_window(Duration::from_secs(3599)).is_ok(),
        "a 1h pre_key_lifetime must still be valid ~1h out"
    );
    assert!(
        lifetime.verify_with_window(Duration::from_secs(3601)).is_err(),
        "a 1h pre_key_lifetime must be expired just past 1h out -- the 90 day default would \
         wrongly still be valid here"
    );
}

/// Sanity check: leaving `spaces_config` unset keeps the previous default (90 day lifetime), so
/// this card's change is additive and doesn't alter default behaviour.
#[tokio::test]
async fn default_spaces_config_keeps_previous_90_day_lifetime() {
    let node = p2panda::builder().spawn().await.expect("node spawns");

    let member = node
        .spaces_manager()
        .me()
        .await
        .expect("own member/key bundle available");
    let lifetime = *member.key_bundle().lifetime();

    // A 1h window is trivially satisfied by a 90 day lifetime.
    assert!(lifetime.verify_with_window(Duration::from_secs(60 * 60)).is_ok());
    // A window just past 90 days must not be (still distinguishes from an absurdly-long/no-op
    // config, i.e. proves the default really is finite and on the order of 90 days).
    assert!(
        lifetime
            .verify_with_window(Duration::from_secs(60 * 60 * 24 * 91))
            .is_err()
    );
}
