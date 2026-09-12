// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::VecDeque;
use std::sync::Mutex;
use std::task::Poll;

use futures_test::task::noop_context;
use tokio::pin;
use tokio::sync::Notify;

/// Simple async queue which awaits when trying to pop from it while it is empty.
///
/// The queue itself lives behind a plain (non-async) `Mutex` that is only ever held for the
/// duration of a `VecDeque` operation, never across an `.await` -- so callers can hold `&self`
/// (not `&mut self`) and no `RefCell` borrow is ever alive while `pop`'s `notify.notified().await`
/// suspends, which is what a `RefCell`-wrapped, `&mut self`-taking version of this type would
/// require (clippy's `await_holding_refcell_ref`).
#[derive(Debug, Default)]
pub struct AsyncBuffer<T> {
    queue: Mutex<VecDeque<T>>,
    notify: Notify,
}

impl<T> AsyncBuffer<T> {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
        }
    }

    pub fn push(&self, item: T) {
        self.queue.lock().unwrap().push_back(item);
        self.notify.notify_one(); // Wake up any pending recv
    }

    pub async fn pop(&self) -> T {
        loop {
            if let Some(item) = self.queue.lock().unwrap().pop_front() {
                return item;
            }

            // Wait for notification that an item was added.
            self.notify.notified().await;
        }
    }

    #[allow(dead_code)]
    pub fn try_pop(&self) -> Option<T> {
        self.queue.lock().unwrap().pop_front()
    }
}

/// Compare the resulting poll state from a future.
pub fn assert_poll_eq<Fut: Future>(fut: Fut, poll: Poll<Fut::Output>)
where
    <Fut as Future>::Output: PartialEq + std::fmt::Debug,
{
    assert_eq!(
        {
            pin!(fut);
            let mut cx = noop_context();
            fut.poll(&mut cx)
        },
        poll,
    );
}
