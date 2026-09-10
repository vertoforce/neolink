use super::BcSubscription;
use crate::{bc::model::*, Error, Result};
use futures::future::BoxFuture;
use futures::sink::{Sink, SinkExt};
use futures::stream::{Stream, StreamExt};
use log::*;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use tokio::{sync::RwLock, task::JoinSet};

type MsgHandler = dyn 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>;

#[derive(Default)]
struct Subscriber {
    /// Subscribers based on their ID and their num
    /// First filtered by ID then number
    /// If num is None it will be upgraded to a Some based on the number the
    /// camera assigns
    num: BTreeMap<u32, BTreeMap<Option<u16>, Sender<Result<Bc>>>>,
    /// Subscribers based on their ID
    id: BTreeMap<u32, Arc<MsgHandler>>,
}

pub(crate) type BcConnSink = Box<dyn Sink<Bc, Error = Error> + Send + Sync + Unpin>;
pub(crate) type BcConnSource = Box<dyn Stream<Item = Result<Bc>> + Send + Sync + Unpin>;

/// A shareable connection to a camera.  Handles serialization of messages.  To send/receive, call
/// .[subscribe()] with a message number.  You can use the BcSubscription to send or receive only
/// messages with that number; each incoming message is routed to its appropriate subscriber.
///
/// There can be only one subscriber per kind of message at a time.
pub struct BcConnection {
    sink: Sender<Result<Bc>>,
    poll_commander: Sender<PollCommand>,
    rx_thread: RwLock<JoinSet<Result<()>>>,
    cancel: CancellationToken,
}

impl BcConnection {
    pub async fn new(mut sink: BcConnSink, mut source: BcConnSource) -> Result<BcConnection> {
        let (sinker, sinker_rx) = channel::<Result<Bc>>(500);
        let cancel = CancellationToken::new();

        let (poll_commander, poll_commanded) = channel(1000);
        let mut poller = Poller {
            subscribers: Default::default(),
            sink: sinker.clone(),
            reciever: ReceiverStream::new(poll_commanded),
        };

        let mut rx_thread = JoinSet::<Result<()>>::new();
        let thread_poll_commander = poll_commander.clone();
        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => {
                    Result::Ok(())
                },
                v = async {
                    let sender = thread_poll_commander;
                    while let Some(bc) = source.next().await {
                        sender.send(PollCommand::Bc(Box::new(bc))).await?;
                    }
                    Result::Ok(())
                } => v
            }
        });

        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    let mut stream = ReceiverStream::new(sinker_rx);
                    while let Some(packet) = stream.next().await {
                        sink.send(packet?).await?;
                    }
                    Ok(())
                } => v
            }
        });

        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                // fix 12: run the poller exactly once. Poller::run() is itself a
                // loop over all commands that returns only on a terminal event:
                // Err (Disconnect / decode failure) OR Ok(()) when its command
                // receiver (poll_commanded) is permanently closed — i.e. every
                // poll_commander sender has dropped and the BcConnection is being
                // torn down. The old `loop { if Err => return }` re-invoked run()
                // on Ok(()); once the receiver closed, run() returned Ok(())
                // immediately every iteration → a tight busy-loop pegging one core
                // at 100% (kempson's Frigate-LXC measurement; we carried the
                // byte-identical bug), and this select's cancel arm could never
                // preempt the hot inner future. A single call is complete: its
                // result is the poller's final result.
                v = poller.run() => {
                    trace!("Polling has ended");
                    v
                }
            }
        });

        Ok(BcConnection {
            sink: sinker,
            poll_commander,
            rx_thread: RwLock::new(rx_thread),
            cancel,
        })
    }

    pub(super) async fn send(&self, bc: Bc) -> crate::Result<()> {
        self.sink.send(Ok(bc)).await?;
        Ok(())
    }

    pub async fn subscribe(&self, msg_id: u32, msg_num: u16) -> Result<BcSubscription> {
        // 500 (upstream PR #399) -> 1000. PR #399 raised this from 100 to 500;
        // on our 4K/5MP HEVC cameras 500 was still short. Chronic ~10/s
        // "Subscriber channel full" drops on camera_d video
        // (MSG_ID_VIDEO=3) showed brief consumer pauses (GST appsrc / RTSP
        // server hiccups) outrun it: 500 slots is ~1.1 s of buffering at
        // 5 Mbps HEVC 20 fps, 1000 is ~2.2 s. RAM cost ~1.4 KB per slot
        // (~1.4 MB per subscriber) is trivial. Net-new tuning on top of #399.
        let (tx, rx) = channel(1000);
        self.poll_commander
            .send(PollCommand::AddSubscriber(msg_id, Some(msg_num), tx))
            .await?;
        Ok(BcSubscription::new(rx, Some(msg_num as u32), self))
    }

    /// Some messages are initiated by the camera. This creates a handler for them
    /// It requires a closure that will be used to handle the message
    /// and return either None or Some(Bc) reply
    pub async fn handle_msg<T>(&self, msg_id: u32, handler: T) -> Result<()>
    where
        T: 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>,
    {
        self.poll_commander
            .send(PollCommand::AddHandler(msg_id, Arc::new(handler)))
            .await?;
        Ok(())
    }

    /// Stop a message handler created using [`handle_msg`]
    #[allow(dead_code)] // Currently unused but added for future use
    pub async fn unhandle_msg(&self, msg_id: u32) -> Result<()> {
        self.poll_commander
            .send(PollCommand::RemoveHandler(msg_id))
            .await?;
        Ok(())
    }

    /// Some times we want to wait for a reply on a new message ID
    /// to do this we wait for the next packet with a certain ID
    /// grab it's message ID and then subscribe to that ID
    ///
    /// The command Snap that grabs a jpeg payload is an example of this
    ///
    /// This function creates a temporary handle to grab this single message
    pub async fn subscribe_to_id(&self, msg_id: u32) -> Result<BcSubscription> {
        // Same bump as subscribe() above - 500 -> 1000 to absorb consumer pauses.
        let (tx, rx) = channel(1000);
        self.poll_commander
            .send(PollCommand::AddSubscriber(msg_id, None, tx))
            .await?;
        Ok(BcSubscription::new(rx, None, self))
    }

    pub(crate) async fn join(&self) -> Result<()> {
        let mut locked_threads = self.rx_thread.write().await;
        while let Some(res) = locked_threads.join_next().await {
            match res {
                Err(e) => {
                    locked_threads.abort_all();
                    return Err(e.into());
                }
                Ok(Err(e)) => {
                    locked_threads.abort_all();
                    return Err(e);
                }
                Ok(Ok(())) => {}
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.poll_commander.send(PollCommand::Disconnect).await;
        self.cancel.cancel();
        let mut locked_threads = self.rx_thread.write().await;
        while locked_threads.join_next().await.is_some() {}
        Ok(())
    }
}

impl Drop for BcConnection {
    fn drop(&mut self) {
        log::trace!("Drop BcConnection");
        self.cancel.cancel();

        let poll_commander = self.poll_commander.clone();
        let _gt = tokio::runtime::Handle::current().enter();
        let mut threads = std::mem::take(&mut self.rx_thread);
        tokio::task::spawn(async move {
            let _ = poll_commander.send(PollCommand::Disconnect).await;
            let locked_threads = threads.get_mut();
            while locked_threads.join_next().await.is_some() {}
            log::trace!("Dropped BcConnection");
        });
    }
}

enum PollCommand {
    Bc(Box<Result<Bc>>),
    AddHandler(u32, Arc<MsgHandler>),
    RemoveHandler(u32),
    AddSubscriber(u32, Option<u16>, Sender<Result<Bc>>),
    Disconnect,
}

impl std::fmt::Debug for PollCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollCommand::Bc(_) => f.write_str("PollCommand::Bc"),
            PollCommand::AddHandler(_, _) => f.write_str("PollCommand::AddHandler"),
            PollCommand::RemoveHandler(_) => f.write_str("PollCommand::RemoveHandler"),
            PollCommand::AddSubscriber(_, _, _) => f.write_str("PollCommand::AddSubscriber"),
            PollCommand::Disconnect => f.write_str("PollCommand::Disconnect"),
        }
    }
}

struct Poller {
    subscribers: Subscriber,
    sink: Sender<Result<Bc>>,
    reciever: ReceiverStream<PollCommand>,
}

impl Poller {
    async fn run(&mut self) -> Result<()> {
        let cancel = CancellationToken::new();
        let _dropguard = cancel.clone().drop_guard();
        while let Some(command) = self.reciever.next().await {
            // Clean Up subscribers
            self.subscribers
                .num
                .iter_mut()
                .for_each(|(_, channels)| channels.retain(|_, channel| !channel.is_closed()));
            self.subscribers
                .num
                .retain(|_, channels| !channels.is_empty());
            // Handle the command
            match command {
                PollCommand::Bc(boxed_response) => {
                    match *boxed_response {
                        Ok(response) => {
                            let msg_id = response.meta.msg_id;
                            let msg_num = response.meta.msg_num;
                            log::trace!(
                                "Looking for ID: {} with num: {}, in {:?} and {:?}",
                                msg_id,
                                msg_num,
                                self.subscribers.id.keys().to_owned(),
                                self.subscribers
                                    .num
                                    .iter()
                                    .map(|(k, v)| (k, v.keys()))
                                    .collect::<Vec<_>>(),
                            );
                            match (
                                self.subscribers.id.get(&msg_id),
                                self.subscribers.num.get_mut(&msg_id), // Both filter first on ID
                            ) {
                                (Some(occ), _) => {
                                    log::trace!("Calling ID callback");
                                    let occ = occ.clone();
                                    let sink = self.sink.clone();
                                    // Move this on another thread coz I have NO idea
                                    // how long the callback will run for
                                    // and we must NOT hang
                                    let cancel = cancel.clone();
                                    tokio::task::spawn(async move {
                                        tokio::select! {
                                            _ = cancel.cancelled() => Result::Ok(()),
                                            v = occ(&response) => {
                                                if let Some(reply) = v {
                                                    assert!(reply.meta.msg_num == response.meta.msg_num);
                                                    sink.send(Ok(reply)).await?;
                                                }
                                                Result::Ok(())
                                            }
                                        }
                                    });
                                    log::trace!("Called ID callback");
                                }
                                (None, Some(occ)) => {
                                    let sender = if let Some(sender) =
                                        occ.get(&Some(msg_num)).filter(|a| !a.is_closed()).cloned()
                                    {
                                        // Connection with id exists and is not closed
                                        Some(sender)
                                    } else if let Some(sender) = occ.get(&None).cloned() {
                                        // Upgrade a None to a known MsgID
                                        occ.remove(&None);
                                        occ.insert(Some(msg_num), sender.clone());
                                        Some(sender)
                                    } else if occ
                                        .get(&Some(msg_num))
                                        .map(|a| a.is_closed())
                                        .unwrap_or(false)
                                    {
                                        // Connection is closed and there is no None to replace it
                                        // Remove it for cleanup and report no sender
                                        occ.remove(&Some(msg_num));
                                        None
                                    } else {
                                        None
                                    };
                                    if let Some(sender) = sender {
                                        if sender.capacity() == 0 {
                                            // Channel is full. Use try_send to avoid blocking
                                            // the message loop, which would prevent keepalive
                                            // ping processing and cause camera disconnection.
                                            match sender.try_send(Ok(response)) {
                                                Ok(()) => {
                                                    trace!(
                                                        "Sent to full channel for {} (ID: {})",
                                                        &msg_num,
                                                        &msg_id
                                                    );
                                                }
                                                Err(_) => {
                                                    warn!(
                                                        "Channel full, dropping message for {} (ID: {}), capacity: {}",
                                                        &msg_num,
                                                        &msg_id,
                                                        sender.max_capacity()
                                                    );
                                                }
                                            }
                                        } else {
                                            trace!(
                                                "Remaining: {} of {} message space for {} (ID: {})",
                                                sender.capacity(),
                                                sender.max_capacity(),
                                                &msg_num,
                                                &msg_id
                                            );
                                            let _ = sender.send(Ok(response)).await;
                                        }
                                    } else {
                                        trace!(
                                            "Ignoring uninteresting message id {} (number: {})",
                                            msg_id,
                                            msg_num
                                        );
                                        trace!("Contents: {:?}", response);
                                    }
                                }
                                (None, None) => {
                                    trace!(
                                        "Ignoring uninteresting message id {} (number: {})",
                                        msg_id,
                                        msg_num
                                    );
                                    trace!("Contents: {:?}", response);
                                }
                            }
                        }
                        Err(e) => {
                            for sub in self.subscribers.num.values() {
                                for sender in sub.values() {
                                    let _ = sender.send(Err(e.clone())).await;
                                }
                            }
                            self.subscribers.num.clear();
                            self.subscribers.id.clear();
                            return Err(e);
                        }
                    }
                }
                PollCommand::AddHandler(msg_id, handler) => {
                    match self.subscribers.id.entry(msg_id) {
                        Entry::Vacant(vac_entry) => {
                            vac_entry.insert(handler);
                        }
                        Entry::Occupied(_) => {
                            return Err(Error::SimultaneousSubscriptionId { msg_id });
                        }
                    };
                }
                PollCommand::RemoveHandler(msg_id) => {
                    self.subscribers.id.remove(&msg_id);
                }
                PollCommand::AddSubscriber(msg_id, msg_num, tx) => {
                    match self
                        .subscribers
                        .num
                        .entry(msg_id)
                        .or_default()
                        .entry(msg_num)
                    {
                        Entry::Vacant(vac_entry) => {
                            vac_entry.insert(tx);
                        }
                        Entry::Occupied(mut occ_entry) => {
                            if occ_entry.get().is_closed() {
                                occ_entry.insert(tx);
                            } else {
                                // log::error!("Failed to subscribe in bcconn to {:?} for {:?}", msg_num, msg_id);
                                let _ = tx
                                    .send(Err(Error::SimultaneousSubscription { msg_num }))
                                    .await;
                            }
                        }
                    };
                }
                PollCommand::Disconnect => {
                    return Err(Error::ConnectionShutdown);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Regression tests for the two net-new fixes in this file:
    //!
    //! * **fix 12** (`81dc9a0`) — the poller supervisor busy-spun once the
    //!   command channel closed.
    //! * **subscriber depth 1000** (`ed4715b`) — one notch above upstream
    //!   PR #399's 500.
    //!
    //! Where a test needs the pre-fix behaviour for comparison it reconstructs
    //! the old expression inline and says so; the base tree is commit
    //! `8708608` (upstream master + PRs #373/#400/#399/#398).

    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};

    /// A `BcConnSink` that swallows every message. The tests here never
    /// inspect what the connection would have sent.
    struct NullSink;

    impl Sink<Bc> for NullSink {
        type Error = Error;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, _: Bc) -> Result<()> {
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn null_sink() -> BcConnSink {
        Box::new(NullSink)
    }

    /// A source that is already at end-of-stream, so the connection's
    /// source-reading task finishes immediately and drops its clone of
    /// `poll_commander`.
    fn empty_source() -> BcConnSource {
        Box::new(futures::stream::empty::<Result<Bc>>())
    }

    fn test_poller(depth: usize) -> (Sender<PollCommand>, Poller) {
        let (cmd_tx, cmd_rx) = channel::<PollCommand>(depth);
        let (sink_tx, sink_rx) = channel::<Result<Bc>>(depth);
        // Keep the sink receiver alive for the life of the poller.
        std::mem::forget(sink_rx);
        (
            cmd_tx,
            Poller {
                subscribers: Default::default(),
                sink: sink_tx,
                reciever: ReceiverStream::new(cmd_rx),
            },
        )
    }

    fn a_video_message(msg_num: u16) -> Bc {
        Bc::new_from_meta(BcMeta {
            msg_id: 3,
            channel_id: 0,
            stream_type: 0,
            response_code: 200,
            msg_num,
            class: 0x6414,
        })
    }

    /// utime+stime of one thread, in clock ticks (100 Hz on Linux/x86-64), read
    /// from `/proc/self/task/<tid>/stat`. Per-*thread* on purpose: `cargo test`
    /// runs test functions in parallel in one process, so a process-wide
    /// counter would be polluted by whatever else is running.
    fn thread_cpu_ticks(tid: u64) -> u64 {
        let stat = std::fs::read_to_string(format!("/proc/self/task/{tid}/stat"))
            .expect("this test needs procfs");
        // Skip past "comm", which may itself contain spaces and parentheses.
        let after_comm = &stat[stat.rfind(')').expect("malformed stat") + 2..];
        let fields: Vec<&str> = after_comm.split_whitespace().collect();
        // fields[0] is `state` (stat field 3), so utime (14) is fields[11] and
        // stime (15) is fields[12].
        fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
    }

    fn this_thread_tid() -> u64 {
        let stat = std::fs::read_to_string("/proc/thread-self/stat").expect("this test needs procfs");
        stat.split_whitespace().next().unwrap().parse().unwrap()
    }

    /// The precondition that made the pre-fix12 supervisor loop hot:
    /// `Poller::run` returns `Ok(())` — not `Pending`, not an error — the
    /// instant its command receiver is closed and drained, and it does so
    /// again on every subsequent call.
    #[tokio::test]
    async fn poller_run_returns_ok_as_soon_as_its_command_channel_closes() {
        let (cmd_tx, mut poller) = test_poller(8);
        drop(cmd_tx);

        for attempt in 0..5 {
            let started = Instant::now();
            let result = poller.run().await;
            assert!(
                result.is_ok(),
                "attempt {attempt}: expected Ok on a closed channel, got {result:?}"
            );
            assert!(
                started.elapsed() < Duration::from_millis(50),
                "attempt {attempt}: run() took {:?}, so it did not return immediately",
                started.elapsed()
            );
        }
    }

    /// The bug itself, measured. This is the exact pre-fix12 supervisor
    /// expression from `BcConnection::new`
    ///
    /// ```ignore
    /// loop { if let n @ Err(_) = poller.run().await { return n; } }
    /// ```
    ///
    /// with one addition: a deadline, so the test terminates. The original had
    /// no exit at all — that is the bug. On a closed channel it re-enters
    /// `run()` as fast as the CPU allows and never yields, so `select!`'s
    /// cancellation arm can never be polled.
    #[tokio::test]
    async fn pre_fix_supervisor_loop_spins_on_a_closed_command_channel() {
        let (cmd_tx, mut poller) = test_poller(8);
        drop(cmd_tx);

        let window = Duration::from_millis(200);
        let deadline = Instant::now() + window;
        let mut re_entries: u64 = 0;
        loop {
            re_entries += 1;
            if poller.run().await.is_err() {
                break;
            }
            if Instant::now() >= deadline {
                break; // not in the original
            }
        }

        eprintln!(
            "pre-fix supervisor re-entered Poller::run {re_entries} times in {} ms",
            window.as_millis()
        );
        assert!(
            re_entries > 10_000,
            "expected a hot loop, only got {re_entries} re-entries in {window:?}"
        );
    }

    /// Teardown must leave no thread burning CPU. Measured on the connection's
    /// own runtime thread (`/proc/self/task/<tid>/stat`) for 500 ms after the
    /// `BcConnection` is dropped.
    ///
    /// MEASURED, both trees, 0 ticks: this passes on base `8708608` too. The
    /// pre-fix loop spun only on `Poller::run` returning `Ok(())`, i.e. the
    /// command channel closing with every sender gone, and `Drop` does not
    /// produce that state — it clones `poll_commander` into a task that sends
    /// `PollCommand::Disconnect`, so `run()` returns `Err` and even the old
    /// loop terminated. The `Ok(())` close is reachable only if every sender is
    /// dropped without a `Disconnect` (the state kempson measured pegging a
    /// core on a Frigate LXC); no route to it through this type's public API
    /// was found. So this is a teardown-hygiene guard, not the bug repro — the
    /// bug itself is measured by
    /// `pre_fix_supervisor_loop_spins_on_a_closed_command_channel`.
    #[test]
    fn dropping_a_connection_leaves_no_spinning_runtime_thread() {
        let (tid_tx, tid_rx) = std::sync::mpsc::channel::<u64>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        // A dedicated current-thread runtime: everything the connection spawns
        // runs on this one thread, so its CPU accounting is the whole story. It
        // is deliberately detached — if the poller does spin, `block_on` never
        // returns and joining would hang the test instead of failing it.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                tid_tx.send(this_thread_tid()).unwrap();
                let conn = BcConnection::new(null_sink(), empty_source())
                    .await
                    .expect("connection");
                // Let the source task reach end-of-stream and drop its clone of
                // poll_commander.
                tokio::time::sleep(Duration::from_millis(100)).await;
                drop(conn);
                // Stay alive long enough to be measured from outside.
                tokio::time::sleep(Duration::from_secs(5)).await;
                let _ = done_tx.send(());
            });
        });

        let tid = tid_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        std::thread::sleep(Duration::from_millis(300)); // setup + drop
        let before = thread_cpu_ticks(tid);
        let window = Duration::from_millis(500);
        std::thread::sleep(window);
        let after = thread_cpu_ticks(tid);
        let burned = after - before;

        eprintln!(
            "connection runtime thread burned {burned} CPU ticks (~{} ms) in the {} ms after drop",
            burned * 10,
            window.as_millis()
        );
        assert!(
            burned < 10,
            "runtime thread burned {burned} ticks (~{} ms) of CPU in {window:?} after the \
             connection was dropped — the poller is spinning",
            burned * 10
        );
        let _ = done_rx.recv_timeout(Duration::from_secs(10));
    }

    /// The depth `subscribe()` actually hands out, measured end to end through
    /// the real connection: feed the source more messages than the channel can
    /// hold while the consumer reads none, then count what survived.
    ///
    /// `ed4715b` sets this to 1000, one notch above upstream PR #399's 500.
    ///
    /// The commit that raised it cites ~10 dropped messages a second on a 4K
    /// HEVC camera at depth 500. That figure is from an earlier deployment and
    /// was NOT reproducible this session: the running fleet already carries
    /// depth 1000, and Loki shows 0 "Channel full, dropping message" lines
    /// across all four cameras over 7 days. So the number below is what is
    /// measured here; the 500-era rate is taken on trust.
    #[tokio::test]
    async fn subscribe_hands_out_a_thousand_deep_channel() {
        let (src_tx, src_rx) = channel::<Result<Bc>>(4096);
        let conn = BcConnection::new(null_sink(), Box::new(ReceiverStream::new(src_rx)))
            .await
            .expect("connection");
        let mut sub = conn.subscribe(3, 1).await.expect("subscribe");

        // Overfill by a wide margin, and never read from `sub`.
        for _ in 0..1500 {
            src_tx.send(Ok(a_video_message(1))).await.unwrap();
        }
        // Let the poller drain everything it is going to drain.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut held = 0;
        while tokio::time::timeout(Duration::from_millis(20), sub.recv())
            .await
            .map(|r| r.is_ok())
            .unwrap_or(false)
        {
            held += 1;
        }
        eprintln!("subscriber held {held} of 1500 messages before the poller started dropping");
        assert_eq!(held, 1000, "subscriber channel depth");
    }

    /// What the depth buys, measured rather than asserted: a consumer that
    /// pauses while the camera keeps pushing loses everything past the channel
    /// depth, because `Poller` deliberately `try_send`s and drops rather than
    /// blocking its message loop (blocking there is what starves the keepalive
    /// ping and disconnects the camera).
    ///
    /// A 1000-frame burst is lossless at depth 1000 and loses half of itself at
    /// PR #399's depth of 500.
    #[tokio::test]
    async fn a_thousand_message_burst_is_lossless_at_depth_1000_and_lossy_at_500() {
        async fn delivered(depth: usize, burst: usize) -> usize {
            let (cmd_tx, mut poller) = test_poller(burst + 8);
            let (sub_tx, mut sub_rx) = channel::<Result<Bc>>(depth);
            cmd_tx
                .send(PollCommand::AddSubscriber(3, Some(1), sub_tx))
                .await
                .unwrap();
            for _n in 0..burst {
                cmd_tx
                    .send(PollCommand::Bc(Box::new(Ok(a_video_message(1)))))
                    .await
                    .unwrap_or_else(|_| panic!("queueing message {_n}"));
            }
            drop(cmd_tx);
            // Drains every queued command, then returns Ok on the closed channel.
            poller.run().await.expect("poller drained cleanly");

            let mut count = 0;
            while sub_rx.try_recv().is_ok() {
                count += 1;
            }
            count
        }

        let burst = 1000;
        let at_1000 = delivered(1000, burst).await;
        let at_500 = delivered(500, burst).await;
        eprintln!(
            "burst of {burst}: depth 1000 delivered {at_1000}, depth 500 (PR #399) delivered {at_500}"
        );
        assert_eq!(at_1000, burst, "depth 1000 should absorb the whole burst");
        assert_eq!(at_500, 500, "depth 500 should drop everything past 500");
    }
}
