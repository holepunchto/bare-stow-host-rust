//! The framing of the control and user data channels, wire-compatible with the
//! JavaScript `bare-stow-protocol`.

use std::fmt::Write;

use serde_json::{Map, Value};

/// The tag of a control frame.
pub const CONTROL: u8 = 0x00;

/// The tag of a user data frame.
pub const USER: u8 = 0x01;

/// Encodes a frame of `tag` carrying `payload`.
pub fn encode(tag: u8, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len() + 1).expect("frame is too large");

    let mut frame = Vec::with_capacity(5 + payload.len());

    frame.extend_from_slice(&length.to_le_bytes());
    frame.push(tag);
    frame.extend_from_slice(payload);

    frame
}

/// Encodes a control frame of `kind` with integer `fields`, in order.
pub fn control(kind: &str, fields: &[(&str, i64)]) -> Vec<u8> {
    let mut json = format!("{{\"type\":{}", Value::from(kind));

    for (key, value) in fields {
        let _ = write!(json, ",{}:{value}", Value::from(*key));
    }

    json.push('}');

    encode(CONTROL, json.as_bytes())
}

/// Decodes the JSON object of a control frame, or returns `None` if it isn't
/// one.
pub fn decode_control(payload: &[u8]) -> Option<Map<String, Value>> {
    match serde_json::from_slice(payload) {
        Ok(Value::Object(message)) => Some(message),
        _ => None,
    }
}

/// Decodes frames from a stream of bytes.
#[derive(Debug, Default)]
pub struct Decoder {
    buffer: Vec<u8>,
}

impl Decoder {
    /// Creates a decoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds `data` to the decoder and returns the frames it completes, as
    /// their tag and payload.
    pub fn feed(&mut self, data: &[u8]) -> Vec<(u8, Vec<u8>)> {
        self.buffer.extend_from_slice(data);

        let mut frames = Vec::new();
        let mut offset = 0;

        while self.buffer.len() - offset >= 4 {
            let header = self.buffer[offset..offset + 4].try_into().unwrap();
            let length = u32::from_le_bytes(header) as usize;

            let start = offset + 4;
            let end = start + length;

            if self.buffer.len() < end {
                break;
            }

            if length > 0 {
                frames.push((self.buffer[start], self.buffer[start + 1..end].to_vec()));
            }

            offset = end;
        }

        self.buffer.drain(..offset);

        frames
    }
}
