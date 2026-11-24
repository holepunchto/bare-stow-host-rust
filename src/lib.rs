//! Host side of the bare-stow lifecycle for Rust.

use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Read, Write};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

pub mod frame;

/// The default time a terminating worker gets to stop and exit.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(5);

/// A fault reported by or observed in the worker.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WorkerError {
    /// The kind of the fault: `WORKER_ERROR` for a fault the worker reported,
    /// `WORKER_EXITED` for a worker that exited, `WORKER_KILLED` for a worker
    /// killed by a signal, `WORKER_CLOSED` for a transport that closed before
    /// ready or a write after it ended, `WORKER_TIMEOUT` for a worker that
    /// timed out before ready, and `TRANSPORT_ERROR` for a transport that
    /// failed while reading or writing. A transport that is reset or broken by
    /// the worker going away counts as ending rather than failing.
    pub code: &'static str,
    /// The message of the error.
    pub message: String,
    /// The stack of the error reported by the worker, if any.
    pub stack: Option<String>,
    /// The exit code of a worker that exited without saying why.
    pub exit_code: Option<i32>,
    /// The signal that killed a worker that exited without saying why.
    pub signal: Option<i32>,
    source: Option<Source>,
}

impl WorkerError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            stack: None,
            exit_code: None,
            signal: None,
            source: None,
        }
    }

    fn transport(err: io::Error) -> Self {
        Self {
            message: err.to_string(),
            source: Some(Source(Arc::new(err))),
            ..Self::new("TRANSPORT_ERROR", "")
        }
    }

    fn closed() -> Self {
        Self::new("WORKER_CLOSED", "Worker closed")
    }
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for WorkerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|source| &*source.0 as _)
    }
}

impl From<WorkerError> for io::Error {
    fn from(err: WorkerError) -> Self {
        io::Error::other(err)
    }
}

// Errors that share a source compare equal, as `io::Error` has no equality.
#[derive(Clone, Debug)]
struct Source(Arc<dyn std::error::Error + Send + Sync>);

impl PartialEq for Source {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Source {}

/// Options for wrapping a transport. Use [`Ipc::builder()`] to create one.
#[derive(Debug)]
pub struct Builder {
    timeout: Option<Duration>,
    grace: Duration,
}

impl Builder {
    /// How long to wait for the worker to signal ready before failing
    /// [`Ipc::ready()`] and terminating it. Waits indefinitely by default.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// How long a terminating worker, or one whose transport failed, gets to
    /// stop and exit before `kill` is called. Defaults to [`DEFAULT_GRACE`].
    pub fn grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }

    /// Wraps the host side of a stowed bundle's `transport` and sends the
    /// grace period to the worker. `kill` stops a worker that ignores
    /// `terminate` for the grace period.
    ///
    /// Whoever wraps a transport must call [`Ipc::exited()`] once the worker
    /// exits. The end of a transport that ends without the worker saying why,
    /// or being asked to terminate, is held back until then, so that reads,
    /// [`Ipc::ready()`], and closing fail with the reason instead of ending
    /// cleanly. A transport that fails instead waits for the exit for the
    /// grace period, failing with the exit and the failure as its source, or
    /// calling `kill` and failing with `TRANSPORT_ERROR` if the worker hasn't
    /// exited by then.
    pub fn wrap<T, K>(self, transport: Arc<T>, kill: K) -> Ipc
    where
        T: Send + Sync + 'static,
        for<'a> &'a T: Read + Write,
        K: Fn() + Send + Sync + 'static,
    {
        let shared = Arc::new(Shared {
            state: Mutex::default(),
            changed: Condvar::new(),
            writer: Mutex::new(Box::new(Writer(Arc::clone(&transport)))),
            kill: Box::new(kill),
            grace: self.grace,
        });

        let grace = i64::try_from(self.grace.as_millis()).unwrap_or(i64::MAX);

        let _ = shared.send(&frame::control("configure", &[("grace", grace)]));

        {
            let shared = Arc::clone(&shared);

            thread::spawn(move || shared.read_frames(&*transport));
        }

        if let Some(timeout) = self.timeout {
            let shared = Arc::clone(&shared);

            // A worker whose transport has ended or failed can no longer time out.
            thread::spawn(move || {
                let (state, result) = shared
                    .changed
                    .wait_timeout_while(shared.lock(), timeout, |state| {
                        state.ready.is_none() && !state.ended && state.pending.is_none()
                    })
                    .unwrap_or_else(PoisonError::into_inner);

                drop(state);

                if result.timed_out() {
                    shared.settle_ready(Err(WorkerError::new(
                        "WORKER_TIMEOUT",
                        "Worker timed out before ready",
                    )));
                }
            });
        }

        Ipc { shared }
    }
}

/// Wraps the host side of a stowed bundle's `transport` with the default
/// options. See [`Builder::wrap()`].
pub fn wrap<T, K>(transport: Arc<T>, kill: K) -> Ipc
where
    T: Send + Sync + 'static,
    for<'a> &'a T: Read + Write,
    K: Fn() + Send + Sync + 'static,
{
    Ipc::builder().wrap(transport, kill)
}

/// The host side of a stowed bundle, layering readiness, termination, and exit
/// reporting over a transport.
///
/// Reading yields the user data written by the worker until the transport
/// ends, and fails with a [`WorkerError`] once the worker has reported a fault
/// or exited without saying why. Writing sends user data to the worker, and
/// fails with a `WORKER_CLOSED` [`WorkerError`] once the transport has ended.
pub struct Ipc {
    shared: Arc<Shared>,
}

impl Ipc {
    /// Creates a builder for wrapping a transport.
    pub fn builder() -> Builder {
        Builder {
            timeout: None,
            grace: DEFAULT_GRACE,
        }
    }

    /// Waits for the worker to signal ready.
    ///
    /// # Errors
    ///
    /// Fails if the worker errors, exits, closes, or times out before then, in
    /// which case the worker is terminated.
    pub fn ready(&self) -> Result<(), WorkerError> {
        let state = self.shared.wait_while(|state| state.ready.is_none());

        state.ready.clone().unwrap()
    }

    /// The exit code the worker reported, if any.
    pub fn exit_code(&self) -> Option<i32> {
        self.shared.lock().exit_code
    }

    /// Waits for the transport to end and returns the exit code the worker
    /// reported, if any.
    pub fn wait(&self) -> Option<i32> {
        self.shared.wait_while(|state| !state.closed).exit_code
    }

    /// Asks the worker to terminate and waits for the transport to end,
    /// calling `kill` if the worker hasn't exited within the grace period.
    /// Returns the exit code the worker reported, if any.
    pub fn terminate(&self) -> Option<i32> {
        self.shared.close();
        self.exit_code()
    }

    /// Like [`Ipc::terminate()`], without the exit code.
    pub fn close(&self) {
        self.shared.close();
    }

    /// Reports that the worker has exited, as observed by the transport, such
    /// as the process of a sidecar. Whoever wraps a transport must call it.
    ///
    /// If the worker exited without saying why and was not asked to, the
    /// handle fails with a `WORKER_EXITED` or `WORKER_KILLED` [`WorkerError`]
    /// carrying `exit_code` and `signal`.
    pub fn exited(&self, code: Option<i32>, signal: Option<i32>) {
        let pending = {
            let state = self.shared.lock();

            if state.exit_reported || state.terminating.is_some() || state.failure.is_some() {
                return;
            }

            state.pending.clone()
        };

        let mut error = match signal {
            Some(signal) => WorkerError {
                exit_code: code,
                signal: Some(signal),
                ..WorkerError::new(
                    "WORKER_KILLED",
                    format!("Worker was killed by signal {signal}"),
                )
            },
            None => WorkerError {
                exit_code: code,
                ..WorkerError::new(
                    "WORKER_EXITED",
                    format!("Worker exited with code {}", code.unwrap_or(-1)),
                )
            },
        };

        error.source = pending.map(|pending| Source(Arc::new(pending)));

        self.shared.fail(error);
    }
}

impl fmt::Debug for Ipc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ipc").finish_non_exhaustive()
    }
}

impl Read for &Ipc {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut state = self
            .shared
            .wait_while(|state| state.data.is_empty() && state.failure.is_none() && !state.closed);

        if let Some(chunk) = state.data.front_mut() {
            let n = buf.len().min(chunk.len());

            buf[..n].copy_from_slice(&chunk[..n]);

            if n == chunk.len() {
                state.data.pop_front();
            } else {
                chunk.drain(..n);
            }

            return Ok(n);
        }

        match &state.failure {
            Some(err) => Err(err.clone().into()),
            None => Ok(0),
        }
    }
}

impl Read for Ipc {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self).read(buf)
    }
}

impl Write for &Ipc {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.shared.send(&frame::encode(frame::USER, buf))?;

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Write for Ipc {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self).flush()
    }
}

struct Writer<T>(Arc<T>);

impl<T> Write for Writer<T>
where
    for<'a> &'a T: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self.0).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self.0).flush()
    }
}

#[derive(Default)]
struct State {
    ready: Option<Result<(), WorkerError>>,
    data: VecDeque<Vec<u8>>,
    ended: bool,
    closed: bool,
    exit_code: Option<i32>,
    exit_reported: bool,
    failure: Option<WorkerError>,
    pending: Option<WorkerError>,
    terminating: Option<Instant>,
    killed: bool,
}

impl State {
    fn may_have_crashed(&self) -> bool {
        !self.exit_reported && self.terminating.is_none() && self.failure.is_none()
    }
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    writer: Mutex<Box<dyn Write + Send>>,
    kill: Box<dyn Fn() + Send + Sync>,
    grace: Duration,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait_while(&self, condition: impl FnMut(&mut State) -> bool) -> MutexGuard<'_, State> {
        self.changed
            .wait_while(self.lock(), condition)
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn send(self: &Arc<Self>, frame: &[u8]) -> Result<(), WorkerError> {
        let Err(err) = self
            .writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .write_all(frame)
        else {
            return Ok(());
        };

        Err(self
            .on_transport_error(err)
            .unwrap_or_else(WorkerError::closed))
    }

    // A worker that may have crashed is likely why its transport failed, so its
    // exit is waited for as an end is, killing it if it hasn't exited within
    // the grace period. Returns the failure, if the error is one.
    fn on_transport_error(self: &Arc<Self>, err: io::Error) -> Option<WorkerError> {
        if peer_gone(&err) {
            return None;
        }

        let error = WorkerError::transport(err);

        {
            let mut state = self.lock();

            // The worker already ended, said why it exits, or was asked to.
            if state.ended || state.exit_reported || state.terminating.is_some() {
                return None;
            }

            if state.may_have_crashed() && state.pending.is_none() {
                state.pending = Some(error.clone());
                self.changed.notify_all();
                self.fail_after_grace(error.clone());
            }
        }

        Some(error)
    }

    fn fail_after_grace(self: &Arc<Self>, error: WorkerError) {
        let shared = Arc::clone(self);

        thread::spawn(move || {
            let (state, result) = shared
                .changed
                .wait_timeout_while(shared.lock(), shared.grace, |state| {
                    state.may_have_crashed() && !state.closed
                })
                .unwrap_or_else(PoisonError::into_inner);

            drop(state);

            if result.timed_out() {
                (shared.kill)();
                shared.fail(error);
            }
        });
    }

    fn read_frames(self: &Arc<Self>, mut transport: impl Read) {
        let mut decoder = frame::Decoder::new();
        let mut buffer = vec![0; 65536];

        loop {
            match transport.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    for (tag, payload) in decoder.feed(&buffer[..n]) {
                        self.on_frame(tag, payload);
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => {
                    self.on_transport_error(err);
                    break;
                }
            }
        }

        self.on_close();
    }

    fn on_frame(self: &Arc<Self>, tag: u8, payload: Vec<u8>) {
        if tag == frame::USER {
            let mut state = self.lock();

            if state.failure.is_none() && !payload.is_empty() {
                state.data.push_back(payload);
                self.changed.notify_all();
            }

            return;
        }

        if tag != frame::CONTROL {
            return;
        }

        let Some(message) = frame::decode_control(&payload) else {
            return;
        };

        match message.get("type").and_then(Value::as_str) {
            Some("ready") => {
                self.settle_ready(Ok(()));
            }
            Some("exit") => {
                let code = message
                    .get("code")
                    .and_then(Value::as_i64)
                    .and_then(|code| i32::try_from(code).ok());

                let faulted = {
                    let mut state = self.lock();

                    state.exit_reported = true;
                    state.exit_code = code;
                    self.changed.notify_all();

                    state.failure.is_some()
                };

                // An error frame precedes the exit frame of a worker that faulted.
                if !faulted {
                    self.settle_ready(Err(WorkerError {
                        exit_code: code,
                        ..WorkerError::new(
                            "WORKER_EXITED",
                            format!(
                                "Worker exited with code {} before ready",
                                code.unwrap_or(-1)
                            ),
                        )
                    }));
                }
            }
            Some("error") => {
                let text = |key| message.get(key).and_then(Value::as_str).map(String::from);

                self.fail(WorkerError {
                    stack: text("stack"),
                    ..WorkerError::new("WORKER_ERROR", text("message").unwrap_or_default())
                });
            }
            _ => {}
        }
    }

    // A worker whose transport ends without it saying why or being asked to
    // has crashed, so the end is held back until its exit is reported for
    // readers to fail with.
    fn on_close(self: &Arc<Self>) {
        {
            let mut state = self.lock();

            state.ended = true;

            if state.may_have_crashed() {
                return;
            }

            state.closed = true;
            self.changed.notify_all();
        }

        self.settle_ready(Err(WorkerError::new(
            "WORKER_CLOSED",
            "Worker closed before ready",
        )));
    }

    fn fail(self: &Arc<Self>, error: WorkerError) {
        {
            let mut state = self.lock();

            if state.failure.is_some() {
                return;
            }

            state.failure = Some(error.clone());
            state.closed = state.ended;
            self.changed.notify_all();
        }

        if !self.settle_ready(Err(error)) {
            self.close_in_background();
        }
    }

    // Settling with an error terminates the worker.
    fn settle_ready(self: &Arc<Self>, result: Result<(), WorkerError>) -> bool {
        let failed = result.is_err();

        {
            let mut state = self.lock();

            if state.ready.is_some() {
                return false;
            }

            state.ready = Some(result);
            self.changed.notify_all();
        }

        if failed {
            self.close_in_background();
        }

        true
    }

    fn close_in_background(self: &Arc<Self>) {
        let shared = Arc::clone(self);

        thread::spawn(move || shared.close());
    }

    // The grace period starts with the first call, and later calls wait for the
    // same deadline. A worker whose transport has ended is past asking.
    fn close(self: &Arc<Self>) {
        let mut state = self.lock();

        let deadline = match state.terminating {
            Some(deadline) => deadline,
            None if state.ended => {
                drop(
                    self.changed
                        .wait_while(state, |state| !state.closed)
                        .unwrap_or_else(PoisonError::into_inner),
                );

                return;
            }
            None => {
                let deadline = Instant::now() + self.grace;

                state.terminating = Some(deadline);

                drop(state);

                let _ = self.send(&frame::control("terminate", &[]));

                state = self.lock();

                deadline
            }
        };

        while !state.closed {
            let now = Instant::now();

            if now >= deadline {
                break;
            }

            state = self
                .changed
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }

        if state.closed {
            return;
        }

        if !state.killed {
            state.killed = true;

            drop(state);

            (self.kill)();

            state = self.lock();
        }

        drop(
            self.changed
                .wait_while(state, |state| !state.closed)
                .unwrap_or_else(PoisonError::into_inner),
        );
    }
}

// A peer that goes away resets or breaks the connection rather than ending it,
// which is no different from it closing.
fn peer_gone(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
    )
}
