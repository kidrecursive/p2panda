// SPDX-License-Identifier: MIT OR Apache-2.0

//! Interfaces for implementing sync protocols and managers.
use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::pin::Pin;

use futures_util::Sink;
use futures_util::Stream;
use futures_util::stream;
use p2panda_core::VerifyingKey;
use serde::{Deserialize, Serialize};

use crate::{FromSync, SessionConfig, ToSync};

/// Generic protocol interface which runs over a sink and stream pair.
pub trait Protocol {
    type Output;
    type Error: StdError + Send + Sync + 'static;
    type Message: Serialize + for<'a> Deserialize<'a>;

    fn run(
        self,
        sink: &mut (impl Sink<Self::Message, Error = impl Debug> + Unpin),
        stream: &mut (impl Stream<Item = Result<Self::Message, impl Debug>> + Unpin),
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>;
}

/// Interface for managing sync sessions and consuming events they emit.
#[allow(clippy::type_complexity)]
pub trait Manager<T> {
    type Protocol: Protocol + Send + 'static;
    type Args: Clone + Send + 'static;
    type Message: Clone + Send + 'static;
    type Event: Clone + Debug + Send + 'static;
    type Error: StdError + Send + Sync + 'static;
    /// square-tower fork addition (D3-u, M4-21): the log id type used to identify individual
    /// append-only logs within a topic's resolved (author, log) set. `PartialEq + Clone` are
    /// required so `p2panda-net`'s topic manager can structurally compare two resolved snapshots
    /// (event-driven resync) without needing the full `LogId` trait.
    type LogId: PartialEq + Clone;

    fn from_args(args: Self::Args) -> Self;

    /// square-tower fork addition (D3-u, M4-21): the topic's current resolved (author, log) set,
    /// as of this call -- used by `p2panda-net`'s topic manager to detect drift against a live
    /// session's own `LogsResolved` baseline (event-driven resync, replacing D3-s's unconditional
    /// per-tick session replacement).
    fn resolved_logs(
        &self,
        topic: &T,
    ) -> impl Future<Output = BTreeMap<VerifyingKey, Vec<Self::LogId>>>;

    /// square-tower fork addition (D3-u, M4-21): push notification stream for new associations
    /// made for this topic -- mirrors `TopicStore::subscribe_new_associations`, giving
    /// `p2panda-net`'s topic manager a way to detect drift immediately instead of waiting for the
    /// resync timer, without needing to know the concrete store type. The default implementation
    /// yields nothing (timer-only fallback), matching the store trait's own default.
    fn subscribe_new_associations(
        &self,
        topic: &T,
    ) -> impl Stream<Item = ()> + Send + Unpin + 'static {
        let _ = topic;
        stream::empty()
    }

    /// square-tower fork addition (D3-u, M4-21): extracts the resolved log set from an event if
    /// it is the fork's `LogsResolved` baseline-snapshot event -- mirrors `is_catch_up_finished`'s
    /// existing pattern (a static event-inspection hook) so `p2panda-net`'s topic manager can
    /// capture a session's baseline snapshot without needing to know the concrete `Event` type.
    /// The default implementation never recognises a baseline (a session's `session_logs` entry
    /// then stays permanently absent, which is treated as "not yet resolved" -- see the Resync
    /// handler's stale-session filter -- a safe, inert fallback for managers that never emit one).
    fn resolved_logs_from_event(
        event: &Self::Event,
    ) -> Option<BTreeMap<VerifyingKey, Vec<Self::LogId>>> {
        let _ = event;
        None
    }

    /// Instantiate a new sync session.
    fn session(
        &mut self,
        session_id: u64,
        config: &SessionConfig<T>,
    ) -> impl Future<Output = Self::Protocol>;

    /// Retrieve a send handle to an already existing sync session.
    fn session_handle(
        &self,
        session_id: u64,
    ) -> impl Future<Output = Option<Pin<Box<dyn Sink<ToSync<Self::Message>, Error = Self::Error>>>>>;

    /// Subscribe to the manager event stream.
    fn subscribe(&mut self) -> impl Stream<Item = FromSync<Self::Event>> + Send + Unpin + 'static;

    /// Returns `true` when the given session event marks the end of a session's initial catch-up
    /// (log-diff/history resolve) phase -- i.e. the session has resolved its offered log set and
    /// is about to either enter live mode or end.
    ///
    /// square-tower fork addition (D3-s, `docs/upstream/p2panda-resync-replaces-session.md`): used
    /// by `p2panda-net`'s `TopicManager` to decide whether an explicit resync (a caller-driven
    /// session replacement, as opposed to the ordinary gossip-driven `Initiate`) may safely close
    /// and replace a still-live session -- doing so while catch-up is in progress would drop
    /// operations the peer hasn't fully caught up on yet.
    fn is_catch_up_finished(event: &Self::Event) -> bool;
}
