use anyhow::Result;

#[derive(Debug, Clone)]
pub enum MuxedStreamFlag {
    NewStream,
    Message,
    CloseStream,
    Disconnected,
    HandshakeReq,
    HandshakeRes,
    Liveliness,
}

impl MuxedStreamFlag {
    pub fn tag(&self) -> u8 {
        match self {
            Self::NewStream => 1,
            Self::Message => 2,
            Self::CloseStream => 3,
            Self::Disconnected => 4,
            Self::HandshakeReq => 5,
            Self::HandshakeRes => 6,
            Self::Liveliness => 7,
        }
    }

    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::NewStream),
            2 => Some(Self::Message),
            3 => Some(Self::CloseStream),
            4 => Some(Self::Disconnected),
            5 => Some(Self::HandshakeReq),
            6 => Some(Self::HandshakeRes),
            7 => Some(Self::Liveliness),
            _ => None,
        }
    }
}

pub fn process_header(buf: &Vec<u8>) -> Result<(u32, MuxedStreamFlag, usize, usize)> {
    // decode varint header
    let (header_val, remaining) = unsigned_varint::decode::u32(buf.as_slice())
        .map_err(|_| anyhow::anyhow!("invalid varint header"))?;

    // How many bytes were consumed?
    let header_len = buf.len() - remaining.len();

    let tag = (header_val & 0b111) as u8;
    let stream_id = header_val >> 3;

    let (payload_len, _) =
        unsigned_varint::decode::usize(remaining).map_err(|_| anyhow::anyhow!("Invalid length"))?;

    let flag =
        MuxedStreamFlag::from_tag(tag).ok_or_else(|| anyhow::anyhow!("Invalid mplex flag"))?;

    Ok((stream_id, flag, header_len, payload_len))
}

pub fn build_frame(stream_id: u32, flag: MuxedStreamFlag, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();

    // ---- Encode header varint ----
    let header_val = (stream_id << 3) | (flag.tag() as u32);
    let mut header_tmp = [0u8; 5];
    let header_encoded = unsigned_varint::encode::u32(header_val, &mut header_tmp);
    buf.extend_from_slice(header_encoded);

    // ---- Encode length varint ----
    let mut len_tmp = [0u8; 10];
    let len_encoded = unsigned_varint::encode::usize(payload.len(), &mut len_tmp);
    buf.extend_from_slice(len_encoded);

    // ---- Payload ----
    buf.extend_from_slice(payload);

    buf
}
