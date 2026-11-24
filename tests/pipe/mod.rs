#![allow(dead_code)]

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use bare_stow_host::frame;
use serde_json::{Map, Value};

#[derive(Default)]
struct Channel {
    state: Mutex<(VecDeque<u8>, bool)>,
    changed: Condvar,
}

pub struct End {
    rx: Arc<Channel>,
    tx: Arc<Channel>,
}

impl End {
    pub fn close(&self) {
        for channel in [&self.rx, &self.tx] {
            channel.state.lock().unwrap().1 = true;
            channel.changed.notify_all();
        }
    }

    pub fn closed(&self) -> bool {
        self.tx.state.lock().unwrap().1
    }
}

impl Read for &End {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut state = self
            .rx
            .changed
            .wait_while(self.rx.state.lock().unwrap(), |(data, closed)| {
                data.is_empty() && !*closed
            })
            .unwrap();

        let n = buf.len().min(state.0.len());

        for (i, byte) in state.0.drain(..n).enumerate() {
            buf[i] = byte;
        }

        Ok(n)
    }
}

impl Write for &End {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut state = self.tx.state.lock().unwrap();

        if state.1 {
            return Err(io::ErrorKind::BrokenPipe.into());
        }

        state.0.extend(buf);
        self.tx.changed.notify_all();

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn pair() -> (Arc<End>, Arc<End>) {
    let a = Arc::new(Channel::default());
    let b = Arc::new(Channel::default());

    (
        Arc::new(End {
            rx: Arc::clone(&a),
            tx: Arc::clone(&b),
        }),
        Arc::new(End { rx: b, tx: a }),
    )
}

pub struct Worker {
    end: Arc<End>,
    frames: Receiver<(u8, Vec<u8>)>,
}

impl Worker {
    pub fn new(end: Arc<End>) -> Self {
        let (sender, frames) = mpsc::channel();
        let reader = Arc::clone(&end);

        thread::spawn(move || {
            let mut decoder = frame::Decoder::new();
            let mut buffer = [0; 1024];

            while let Ok(n @ 1..) = (&*reader).read(&mut buffer) {
                for frame in decoder.feed(&buffer[..n]) {
                    let _ = sender.send(frame);
                }
            }
        });

        Self { end, frames }
    }

    pub fn send(&self, kind: &str, fields: &[(&str, i64)]) {
        (&*self.end)
            .write_all(&frame::control(kind, fields))
            .unwrap();
    }

    pub fn send_json(&self, json: &str) {
        (&*self.end)
            .write_all(&frame::encode(frame::CONTROL, json.as_bytes()))
            .unwrap();
    }

    pub fn write(&self, data: &[u8]) {
        (&*self.end)
            .write_all(&frame::encode(frame::USER, data))
            .unwrap();
    }

    pub fn receive(&self, kind: &str) -> Map<String, Value> {
        loop {
            let (tag, payload) = self.next();

            if tag == frame::CONTROL {
                let message = frame::decode_control(&payload).unwrap();

                if message["type"] == kind {
                    return message;
                }
            }
        }
    }

    pub fn data(&self) -> Vec<u8> {
        loop {
            let (tag, payload) = self.next();

            if tag == frame::USER {
                return payload;
            }
        }
    }

    pub fn close(&self) {
        self.end.close();
    }

    // Closes the transport once asked to terminate.
    pub fn close_on_terminate(self) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            self.receive("terminate");
            self.close();
        })
    }

    fn next(&self) -> (u8, Vec<u8>) {
        self.frames.recv_timeout(Duration::from_secs(5)).unwrap()
    }
}
