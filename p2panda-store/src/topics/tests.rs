// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::StreamExt;
use p2panda_core::{SigningKey, Topic, VerifyingKey};

use crate::topics::TopicStore;
use crate::{SqliteStore, Transaction};

#[tokio::test]
async fn update_and_resolve_topic_mapping() {
    let store = SqliteStore::temporary().await;

    let topic = Topic::random();

    // The log id is the same as the topic, in a use case like this there will be one log
    // per-author in each topic.
    let log_id = topic;

    let alice = SigningKey::from_bytes(&[1u8; 32]).verifying_key();
    let bob = SigningKey::from_bytes(&[2u8; 32]).verifying_key();

    let permit = store.begin().await.unwrap();

    let result = store.associate(&topic, &alice, &log_id).await.unwrap();
    assert!(result);

    let result = store.associate(&topic, &bob, &log_id).await.unwrap();
    assert!(result);

    store.commit(permit).await.unwrap();

    // Inserting bob again results in a false result.
    let permit = store.begin().await.unwrap();

    let result = store.associate(&topic, &bob, &log_id).await.unwrap();
    assert!(!result);

    store.commit(permit).await.unwrap();

    let expected_logs = BTreeMap::from([(alice, vec![topic]), (bob, vec![topic])]);

    let logs = store.resolve(&topic).await.unwrap();
    assert_eq!(logs, expected_logs);
}

#[tokio::test]
async fn resolve_topics_from_association() {
    let store = SqliteStore::temporary().await;

    let topic = Topic::random();
    let log_id = topic;

    let alice = SigningKey::from_bytes(&[1u8; 32]).verifying_key();
    let bob = SigningKey::from_bytes(&[2u8; 32]).verifying_key();

    let permit = store.begin().await.unwrap();
    let result = store.associate(&topic, &alice, &log_id).await.unwrap();
    assert!(result);
    store.commit(permit).await.unwrap();

    // Resolving a known association returns the topic.
    let resolved: Vec<Topic> = store.resolve_topics(&alice, &log_id).await.unwrap();
    assert_eq!(resolved, vec![topic]);

    // An unknown author/data id pair returns None.
    let resolved: Vec<Topic> = store.resolve_topics(&bob, &log_id).await.unwrap();
    assert_eq!(resolved, vec![]);
}

#[tokio::test]
async fn path_based_log_ids() {
    let store = SqliteStore::temporary().await;

    let topic = Topic::random();

    // Here we demonstrate use cases where there are multiple logs per-author in each topic.
    let log_id_kittens = "kittens".to_string();
    let log_id_kittens_sleepy = "kittens.sleepy".to_string();
    let log_id_puppies = "puppies".to_string();

    let alice = SigningKey::from_bytes(&[1u8; 32]).verifying_key();
    let bob = SigningKey::from_bytes(&[2u8; 32]).verifying_key();

    let permit = store.begin().await.unwrap();

    let result = store
        .associate(&topic, &alice, &log_id_kittens)
        .await
        .unwrap();
    assert!(result);

    let result = store
        .associate(&topic, &alice, &log_id_kittens_sleepy)
        .await
        .unwrap();
    assert!(result);

    let result = store
        .associate(&topic, &bob, &log_id_puppies)
        .await
        .unwrap();
    assert!(result);

    store.commit(permit).await.unwrap();

    let expected_logs = BTreeMap::from([
        (alice, vec![log_id_kittens, log_id_kittens_sleepy]),
        (bob, vec![log_id_puppies]),
    ]);

    let logs = store.resolve(&topic).await.unwrap();
    assert_eq!(logs, expected_logs);
}

#[tokio::test]
async fn remove_association() {
    let store = SqliteStore::temporary().await;

    let topic = Topic::random();

    // Here we demonstrate use cases where there are multiple logs per-author in each topic.
    let log_id_kittens = "kittens".to_string();
    let log_id_kittens_sleepy = "kittens.sleepy".to_string();

    let alice = SigningKey::from_bytes(&[1u8; 32]).verifying_key();

    let permit = store.begin().await.unwrap();

    let result = store
        .associate(&topic, &alice, &log_id_kittens)
        .await
        .unwrap();
    assert!(result);

    let result = store
        .associate(&topic, &alice, &log_id_kittens_sleepy)
        .await
        .unwrap();
    assert!(result);

    store.commit(permit).await.unwrap();

    let expected_logs = BTreeMap::from([(
        alice,
        vec![log_id_kittens.clone(), log_id_kittens_sleepy.clone()],
    )]);

    let logs = store.resolve(&topic).await.unwrap();
    assert_eq!(logs, expected_logs);

    let permit = store.begin().await.unwrap();

    let result = store
        .remove(&topic, &alice, &log_id_kittens_sleepy)
        .await
        .unwrap();

    store.commit(permit).await.unwrap();

    assert!(result);

    let expected_logs = BTreeMap::from([(alice, vec![log_id_kittens])]);

    let logs = store.resolve(&topic).await.unwrap();
    assert_eq!(logs, expected_logs);
}

#[tokio::test]
async fn query_associated_topics() {
    let store = SqliteStore::temporary().await;

    let topic_1 = Topic::random();
    let topic_2 = Topic::random();
    let topic_3 = Topic::random();

    let alice = SigningKey::from_bytes(&[1u8; 32]).verifying_key();
    let bob = SigningKey::from_bytes(&[2u8; 32]).verifying_key();
    let cat = SigningKey::from_bytes(&[3u8; 32]).verifying_key();

    let log_id: String = "kittens".into();

    let permit = store.begin().await.unwrap();

    let result = store.associate(&topic_1, &alice, &log_id).await.unwrap();
    assert!(result);

    let result = store.associate(&topic_2, &alice, &log_id).await.unwrap();
    assert!(result);

    let result = store.associate(&topic_2, &bob, &log_id).await.unwrap();
    assert!(result);

    let result = store.associate(&topic_3, &cat, &log_id).await.unwrap();
    assert!(result);

    let expected_topics = Vec::from([topic_1, topic_2, topic_3]);

    let topics: Vec<Topic> =
        <SqliteStore as TopicStore<Topic, VerifyingKey, String>>::topics(&store)
            .await
            .unwrap();

    store.commit(permit).await.unwrap();

    for topic in expected_topics {
        assert!(topics.contains(&topic));
    }
}

/// square-tower fork addition (D3-u, M4-21): `associate`'s `is_new` case must push exactly one
/// notification on `subscribe_new_associations` -- re-associating an already-known
/// (topic, author, data_id) triple must not fire a second one. This is the sole real association
/// choke point event-driven resync relies on to detect drift without waiting for the resync
/// timer.
///
/// Mutation-proof: dropping the `is_new` guard around the `assoc_tx.send` call in
/// `associate` (i.e. notifying unconditionally) makes the "no duplicate notification" assertion
/// below fail.
#[tokio::test]
async fn associate_sends_notification_only_when_new() {
    let store = SqliteStore::temporary().await;

    let topic = Topic::random();
    let log_id = topic;
    let alice = SigningKey::from_bytes(&[1u8; 32]).verifying_key();

    let mut new_associations =
        <SqliteStore as TopicStore<Topic, VerifyingKey, Topic>>::subscribe_new_associations(
            &store, &topic,
        );

    let permit = store.begin().await.unwrap();
    let result = store.associate(&topic, &alice, &log_id).await.unwrap();
    store.commit(permit).await.unwrap();
    assert!(result, "first association must be new");

    tokio::time::timeout(Duration::from_millis(200), new_associations.next())
        .await
        .expect("a new association must notify within 200ms")
        .expect("stream must not have ended");

    // Re-associating the exact same (topic, author, data_id) triple is not new.
    let permit = store.begin().await.unwrap();
    let result = store.associate(&topic, &alice, &log_id).await.unwrap();
    store.commit(permit).await.unwrap();
    assert!(!result, "re-association of the same triple must not be new");

    let no_duplicate =
        tokio::time::timeout(Duration::from_millis(200), new_associations.next()).await;
    assert!(
        no_duplicate.is_err(),
        "re-associating an already-known triple must not notify again, but got: {no_duplicate:?}"
    );
}
