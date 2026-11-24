# bare-stow-host-rust

Host side of the [bare-stow](https://github.com/holepunchto/bare-stow) lifecycle for Rust. The Rust counterpart of [bare-stow-host](https://github.com/holepunchto/bare-stow-host): it multiplexes a control channel and a user data channel over a byte transport to a stowed bundle, and layers readiness, termination, and exit reporting on top.

```
cargo add bare-stow-host
```

## Usage

```rust
use std::io::Write;
use std::sync::Arc;

fn main() -> std::io::Result<()> {
    let worklet = Arc::new(bare_kit::start("index.bundle")?);

    let ipc = Arc::new(bare_stow_host::wrap(Arc::clone(&worklet), {
        let worklet = Arc::clone(&worklet);

        move || worklet.terminate()
    }));

    std::thread::spawn({
        let worklet = Arc::clone(&worklet);
        let ipc = Arc::clone(&ipc);

        move || ipc.exited(Some(worklet.wait()), None)
    });

    ipc.ready()?;

    (&*ipc).write_all(b"hello")?;

    let code = ipc.terminate();

    Ok(())
}
```

Any `T` where `&T` implements `Read` and `Write` can be wrapped, such as a `Worklet` from [bare-kit-rust](https://github.com/holepunchto/bare-kit-rust). Reading from the handle yields the user data from the worker, and writing to it sends user data to the worker. Use `Ipc::builder()` to set the `timeout` to wait for the worker to signal ready and the `grace` period a terminating worker gets before it's killed.

Whoever wraps a transport must call `Ipc::exited()` once the worker exits. The end of a worker that crashes is held back until its exit is reported, so that reads and `Ipc::ready()` fail with the exit code or signal instead of ending cleanly. A transport that fails instead waits for the exit for the grace period, and the worker is killed if it hasn't exited by then. A transport that is reset or broken by the worker going away counts as ending rather than failing.

## Protocol

Every frame is a 4-byte little-endian length followed by that many bytes: A 1-byte tag and the payload. The tag is `0x00` for a control frame, whose payload is a UTF-8 JSON object carrying its `type`, and `0x01` for a user data frame. See [bare-stow-protocol](https://github.com/holepunchto/bare-stow-protocol) for the control frames.

## License

Apache-2.0
