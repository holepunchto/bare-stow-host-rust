mod pipe;

use std::error::Error;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use bare_stow_host::{Ipc, WorkerError, wrap};
use pipe::{End, Worker, pair};

fn wrap_closing(end: &Arc<End>) -> Ipc {
    let killed = Arc::clone(end);

    wrap(Arc::clone(end), move || killed.close())
}

fn wrap_with(builder: bare_stow_host::Builder, end: &Arc<End>) -> Ipc {
    let killed = Arc::clone(end);

    builder.wrap(Arc::clone(end), move || killed.close())
}

fn read_to_end(ipc: &Ipc) -> io::Result<Vec<u8>> {
    let mut data = Vec::new();

    (&*ipc).read_to_end(&mut data)?;

    Ok(data)
}

fn read_error(ipc: &Ipc) -> WorkerError {
    let err = read_to_end(ipc).unwrap_err();

    err.into_inner()
        .unwrap()
        .downcast_ref::<WorkerError>()
        .unwrap()
        .clone()
}

// Gives the host time to see the end of the transport.
fn settle() {
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn resolves_ready_when_the_worker_signals() {
    let (left, right) = pair();
    let ipc = wrap_closing(&left);
    let worker = Worker::new(right);

    worker.send("ready", &[]);

    assert_eq!(ipc.ready(), Ok(()));

    worker.send("exit", &[("code", 0)]);
    worker.close();
    ipc.wait();
}

#[test]
fn sends_the_grace_to_the_worker() {
    let (left, right) = pair();
    let ipc = wrap_with(Ipc::builder().grace(Duration::from_millis(2500)), &left);
    let worker = Worker::new(right);

    let message = worker.receive("configure");

    assert_eq!(message["grace"], 2500);

    worker.send("exit", &[("code", 0)]);
    worker.close();
    ipc.wait();
}

#[test]
fn rejects_ready_when_the_worker_errors_before_ready() {
    let (left, right) = pair();
    let ipc = wrap_closing(&left);
    let worker = Worker::new(right);

    worker.send_json(r#"{"type":"error","message":"boom","stack":"at core.js"}"#);

    let closing = worker.close_on_terminate();

    let err = ipc.ready().unwrap_err();

    assert_eq!(err.code, "WORKER_ERROR");
    assert_eq!(err.message, "boom");
    assert_eq!(err.stack.as_deref(), Some("at core.js"));

    closing.join().unwrap();
}

#[test]
fn rejects_ready_when_the_worker_exits_before_ready() {
    let (left, right) = pair();
    let ipc = wrap_closing(&left);
    let worker = Worker::new(right);

    worker.send("exit", &[("code", 1)]);

    let closing = worker.close_on_terminate();

    let err = ipc.ready().unwrap_err();

    assert_eq!(err.code, "WORKER_EXITED");
    assert_eq!(err.exit_code, Some(1));

    closing.join().unwrap();
}

#[test]
fn close_before_ready_sends_terminate_and_rejects_ready() {
    let (left, right) = pair();
    let ipc = wrap_closing(&left);
    let closing = Worker::new(right).close_on_terminate();

    ipc.close();

    closing.join().unwrap();

    assert_eq!(ipc.ready().unwrap_err().code, "WORKER_CLOSED");
    assert_eq!(ipc.wait(), None);
}

struct Broken(io::ErrorKind);

impl Read for &Broken {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(self.0.into())
    }
}

impl Write for &Broken {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn fails_a_crashed_worker_with_its_exit_rather_than_a_transport_failure() {
    let ipc = wrap(Arc::new(Broken(io::ErrorKind::Other)), || {});

    settle();

    ipc.exited(None, Some(11));

    let err = ipc.ready().unwrap_err();
    let failure = err.source().unwrap().downcast_ref::<WorkerError>().unwrap();
    let source = failure
        .source()
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();

    assert_eq!(err.code, "WORKER_KILLED");
    assert_eq!(failure.code, "TRANSPORT_ERROR");
    assert_eq!(source.kind(), io::ErrorKind::Other);
}

#[test]
fn kills_a_worker_whose_transport_fails_and_that_does_not_exit() {
    let killed = Arc::new(AtomicBool::new(false));
    let ipc = {
        let killed = Arc::clone(&killed);

        Ipc::builder().grace(Duration::from_millis(10)).wrap(
            Arc::new(Broken(io::ErrorKind::Other)),
            move || {
                killed.store(true, Ordering::SeqCst);
            },
        )
    };

    let err = ipc.ready().unwrap_err();
    let source = err.source().unwrap().downcast_ref::<io::Error>().unwrap();

    assert_eq!(err.code, "TRANSPORT_ERROR");
    assert_eq!(source.kind(), io::ErrorKind::Other);
    assert!(killed.load(Ordering::SeqCst));

    ipc.exited(Some(1), None);

    assert_eq!(read_error(&ipc).code, "TRANSPORT_ERROR");
}

#[test]
fn treats_a_reset_transport_as_closed() {
    let ipc = wrap(Arc::new(Broken(io::ErrorKind::ConnectionReset)), || {});

    settle();

    ipc.exited(Some(1), None);

    let err = ipc.ready().unwrap_err();

    assert_eq!(err.code, "WORKER_EXITED");
    assert!(err.source().is_none());
}

#[test]
fn ignores_exit_after_ready() {
    let (left, right) = pair();
    let ipc = wrap_closing(&left);
    let worker = Worker::new(right);

    worker.send("ready", &[]);
    ipc.ready().unwrap();

    worker.send("exit", &[("code", 1)]);
    worker.close();
    ipc.wait();

    assert_eq!(ipc.ready(), Ok(()));
}

#[test]
fn reports_the_exit_code() {
    let (left, right) = pair();
    let ipc = wrap_closing(&left);
    let worker = Worker::new(right);

    worker.send("ready", &[]);
    worker.send("exit", &[("code", 7)]);
    worker.close();

    assert_eq!(ipc.wait(), Some(7));
    assert_eq!(ipc.exit_code(), Some(7));
}

#[test]
fn carries_user_data() {
    let (left, right) = pair();
    let ipc = wrap_closing(&left);
    let worker = Worker::new(right);

    worker.write(b"payload");

    let mut received = [0; 7];

    (&ipc).read_exact(&mut received).unwrap();

    assert_eq!(&received, b"payload");

    (&ipc).write_all(b"reply").unwrap();

    assert_eq!(worker.data(), b"reply");

    worker.send("exit", &[("code", 0)]);
    worker.close();

    assert_eq!(read_to_end(&ipc).unwrap(), b"");
}

#[test]
fn reads_user_data_in_parts() {
    let (left, right) = pair();
    let ipc = wrap_closing(&left);
    let worker = Worker::new(right);

    worker.write(b"hello");
    worker.write(b"");
    worker.write(b" world");
    worker.send("exit", &[("code", 0)]);
    worker.close();

    let mut part = [0; 3];

    assert_eq!((&ipc).read(&mut part).unwrap(), 3);
    assert_eq!(&part, b"hel");
    assert_eq!(read_to_end(&ipc).unwrap(), b"lo world");
}

#[test]
fn terminate_returns_the_exit_code() {
    let (left, right) = pair();
    let ipc = wrap_closing(&left);
    let worker = Worker::new(right);

    worker.send("ready", &[]);
    ipc.ready().unwrap();

    let exiting = thread::spawn(move || {
        worker.receive("terminate");
        worker.send("exit", &[("code", 0)]);
        worker.close();
    });

    assert_eq!(ipc.terminate(), Some(0));

    exiting.join().unwrap();
}

#[test]
fn rejects_ready_and_terminates_the_worker_on_timeout() {
    let (left, right) = pair();
    let ipc = wrap_with(Ipc::builder().timeout(Duration::from_millis(10)), &left);
    let closing = Worker::new(right).close_on_terminate();

    let err = ipc.ready().unwrap_err();

    assert_eq!(err.code, "WORKER_TIMEOUT");

    closing.join().unwrap();
}

#[test]
fn terminate_calls_kill_after_the_grace_period() {
    let (left, right) = pair();
    let killed = Arc::new(AtomicBool::new(false));

    let ipc = {
        let killed = Arc::clone(&killed);
        let left = Arc::clone(&left);

        Ipc::builder()
            .grace(Duration::from_millis(10))
            .wrap(Arc::clone(&left), move || {
                killed.store(true, Ordering::SeqCst);
                left.close();
            })
    };

    let worker = Worker::new(right);

    worker.send("ready", &[]);
    ipc.ready().unwrap();

    assert_eq!(ipc.terminate(), None);
    assert!(killed.load(Ordering::SeqCst));
}

#[test]
fn terminate_returns_once_the_transport_has_closed() {
    let (left, _right) = pair();
    let ipc = wrap_closing(&left);

    left.close();
    settle();

    ipc.exited(Some(0), None);

    assert!(ipc.ready().is_err());
    assert_eq!(ipc.terminate(), None);
}

#[test]
fn exited_before_ready_fails_ready_with_the_exit_code() {
    let (left, _right) = pair();
    let ipc = wrap_with(Ipc::builder().grace(Duration::from_millis(10)), &left);

    ipc.exited(Some(3), None);

    let err = ipc.ready().unwrap_err();

    assert_eq!(err.code, "WORKER_EXITED");
    assert_eq!(err.exit_code, Some(3));

    ipc.close();
}

#[test]
fn exited_after_ready_fails_reads_with_the_signal() {
    let (left, right) = pair();
    let ipc = wrap_with(Ipc::builder().grace(Duration::from_millis(10)), &left);
    let worker = Worker::new(right);

    worker.send("ready", &[]);
    ipc.ready().unwrap();

    ipc.exited(Some(137), Some(9));

    let err = read_error(&ipc);

    assert_eq!(err.code, "WORKER_KILLED");
    assert_eq!(err.exit_code, Some(137));
    assert_eq!(err.signal, Some(9));

    ipc.close();
}

#[test]
fn exited_is_ignored_after_an_exit_frame() {
    let (left, right) = pair();
    let ipc = wrap_closing(&left);
    let worker = Worker::new(right);

    worker.send("ready", &[]);
    ipc.ready().unwrap();

    worker.send("exit", &[("code", 0)]);
    worker.close();
    ipc.wait();

    ipc.exited(Some(0), None);

    assert_eq!(read_to_end(&ipc).unwrap(), b"");
}

#[test]
fn exited_is_ignored_while_terminating() {
    let (left, right) = pair();
    let ipc = Arc::new(wrap_with(
        Ipc::builder().grace(Duration::from_millis(10)),
        &left,
    ));
    let worker = Worker::new(right);

    worker.send("ready", &[]);
    ipc.ready().unwrap();

    let terminating = {
        let ipc = Arc::clone(&ipc);

        thread::spawn(move || ipc.terminate())
    };

    worker.receive("terminate");

    ipc.exited(None, Some(9));

    assert_eq!(terminating.join().unwrap(), None);
    assert_eq!(read_to_end(&ipc).unwrap(), b"");
}

#[test]
fn holds_the_end_of_a_crashed_worker_until_its_exit_is_reported() {
    let (left, right) = pair();
    let ipc = Arc::new(wrap_closing(&left));
    let worker = Worker::new(right);

    worker.send("ready", &[]);
    ipc.ready().unwrap();

    let reading = {
        let ipc = Arc::clone(&ipc);

        thread::spawn(move || read_error(&ipc))
    };

    worker.close();
    settle();

    let closing = {
        let ipc = Arc::clone(&ipc);

        thread::spawn(move || ipc.close())
    };

    settle();

    ipc.exited(None, Some(11));

    let err = reading.join().unwrap();

    assert_eq!(err.code, "WORKER_KILLED");
    assert_eq!(err.signal, Some(11));

    closing.join().unwrap();
}

#[test]
fn holds_the_end_of_a_worker_that_crashes_before_ready() {
    let (left, right) = pair();
    let ipc = wrap_closing(&left);

    right.close();
    settle();

    ipc.exited(Some(1), None);

    let err = ipc.ready().unwrap_err();

    assert_eq!(err.code, "WORKER_EXITED");
    assert_eq!(err.exit_code, Some(1));
    assert_eq!(ipc.wait(), None);
}

#[test]
fn fails_a_crashed_worker_with_its_exit_rather_than_a_timeout() {
    let (left, right) = pair();
    let ipc = wrap_with(Ipc::builder().timeout(Duration::from_millis(50)), &left);

    right.close();
    thread::sleep(Duration::from_millis(100));

    ipc.exited(Some(1), None);

    assert_eq!(ipc.ready().unwrap_err().code, "WORKER_EXITED");
}

struct Unwritable {
    end: Arc<End>,
    failing: Mutex<Option<io::ErrorKind>>,
}

impl Unwritable {
    fn new(end: Arc<End>) -> Arc<Self> {
        Arc::new(Self {
            end,
            failing: Mutex::new(None),
        })
    }

    fn wrap(self: &Arc<Self>, builder: bare_stow_host::Builder) -> Ipc {
        let transport = Arc::clone(self);

        builder.wrap(Arc::clone(self), move || transport.end.close())
    }

    fn fail(&self, kind: io::ErrorKind) {
        *self.failing.lock().unwrap() = Some(kind);
    }
}

impl Read for &Unwritable {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self.end).read(buf)
    }
}

impl Write for &Unwritable {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(kind) = *self.failing.lock().unwrap() {
            return Err(kind.into());
        }

        (&*self.end).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn write_error(ipc: &Ipc) -> WorkerError {
    let err = (&*ipc).write_all(b"payload").unwrap_err();

    err.into_inner()
        .unwrap()
        .downcast_ref::<WorkerError>()
        .unwrap()
        .clone()
}

#[test]
fn fails_writes_when_the_transport_fails() {
    let (left, right) = pair();
    let transport = Unwritable::new(left);
    let ipc = transport.wrap(Ipc::builder().grace(Duration::from_millis(10)));
    let worker = Worker::new(right);

    worker.send("ready", &[]);
    ipc.ready().unwrap();

    transport.fail(io::ErrorKind::Other);

    let err = write_error(&ipc);
    let source = err.source().unwrap().downcast_ref::<io::Error>().unwrap();

    assert_eq!(err.code, "TRANSPORT_ERROR");
    assert_eq!(source.kind(), io::ErrorKind::Other);
    assert_eq!(read_error(&ipc).code, "TRANSPORT_ERROR");
    assert!(transport.end.closed());
}

#[test]
fn fails_writes_as_closed_and_ends_with_the_reader_when_the_worker_is_gone() {
    let (left, right) = pair();
    let transport = Unwritable::new(left);
    let ipc = transport.wrap(Ipc::builder().grace(Duration::from_millis(10)));
    let worker = Worker::new(right);

    worker.send("ready", &[]);
    ipc.ready().unwrap();

    transport.fail(io::ErrorKind::BrokenPipe);

    assert_eq!(write_error(&ipc).code, "WORKER_CLOSED");

    worker.send("exit", &[("code", 3)]);
    worker.close();

    assert_eq!(ipc.wait(), Some(3));
    assert_eq!(read_to_end(&ipc).unwrap(), b"");
}

#[test]
fn fails_writes_after_the_end_as_closed() {
    let (left, right) = pair();
    let transport = Unwritable::new(left);
    let ipc = transport.wrap(Ipc::builder().grace(Duration::from_millis(10)));
    let worker = Worker::new(right);

    worker.send("ready", &[]);
    ipc.ready().unwrap();

    worker.send("exit", &[("code", 0)]);
    worker.close();
    ipc.wait();

    transport.fail(io::ErrorKind::Other);

    assert_eq!(write_error(&ipc).code, "WORKER_CLOSED");
    assert_eq!(read_to_end(&ipc).unwrap(), b"");
}

#[test]
fn fails_a_worker_whose_control_frame_fails_with_its_exit_rather_than_a_timeout() {
    let (left, _right) = pair();
    let transport = Unwritable::new(left);

    transport.fail(io::ErrorKind::Other);

    let ipc = transport.wrap(Ipc::builder().timeout(Duration::from_millis(50)));

    thread::sleep(Duration::from_millis(100));

    ipc.exited(Some(1), None);

    let err = ipc.ready().unwrap_err();
    let failure = err.source().unwrap().downcast_ref::<WorkerError>().unwrap();

    assert_eq!(err.code, "WORKER_EXITED");
    assert_eq!(failure.code, "TRANSPORT_ERROR");
}
