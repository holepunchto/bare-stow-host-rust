use bare_stow_host::frame::{self, CONTROL, Decoder, USER};

fn hex(input: &str) -> Vec<u8> {
    (0..input.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&input[i..i + 2], 16).unwrap())
        .collect()
}

// Written by the JavaScript bare-stow-protocol for the same frames.
#[test]
fn encodes_frames_like_the_javascript_implementation() {
    assert_eq!(
        frame::control("configure", &[("grace", 5000)]),
        hex("22000000007b2274797065223a22636f6e666967757265222c226772616365223a353030307d")
    );
    assert_eq!(
        frame::control("ready", &[]),
        hex("11000000007b2274797065223a227265616479227d")
    );
    assert_eq!(frame::encode(USER, b"hello"), hex("060000000168656c6c6f"));
}

#[test]
fn decodes_frames_split_across_chunks() {
    let mut decoder = Decoder::new();
    let data = [frame::encode(USER, b"hello"), frame::control("ready", &[])].concat();

    let frames: Vec<_> = data.chunks(1).flat_map(|byte| decoder.feed(byte)).collect();

    assert_eq!(
        frames,
        [
            (USER, b"hello".to_vec()),
            (CONTROL, br#"{"type":"ready"}"#.to_vec())
        ]
    );
}

#[test]
fn decodes_several_frames_from_one_chunk() {
    let mut decoder = Decoder::new();
    let data = [
        frame::encode(USER, b"a"),
        frame::encode(USER, b""),
        frame::encode(USER, b"b"),
    ]
    .concat();

    assert_eq!(
        decoder.feed(&data),
        [(USER, b"a".to_vec()), (USER, vec![]), (USER, b"b".to_vec())]
    );
}

#[test]
fn skips_empty_frames() {
    let mut decoder = Decoder::new();
    let data = [vec![0, 0, 0, 0], frame::encode(USER, b"a")].concat();

    assert_eq!(decoder.feed(&data), [(USER, b"a".to_vec())]);
}

#[test]
fn ignores_invalid_control_payloads() {
    assert_eq!(frame::decode_control(b"not json"), None);
    assert_eq!(frame::decode_control(b"[1, 2]"), None);
    assert_eq!(
        frame::decode_control(br#"{"type":"ready"}"#).unwrap()["type"],
        "ready"
    );
}
