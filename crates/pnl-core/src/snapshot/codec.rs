use super::SnapshotV2;
use crate::error::{Error, Result};
use std::io::{Read, Write};

const SNAPSHOT_MAGIC: &[u8; 8] = b"PNLRS001";
const SNAPSHOT_VERSION: u16 = 2;
const SNAPSHOT_CODEC_POSTCARD: u8 = 1;
const SNAPSHOT_COMPRESSION_NONE: u8 = 0;
const HEADER_LEN: usize = 56;

pub(super) fn write_snapshot<W: Write>(snapshot: &SnapshotV2, mut writer: W) -> Result<()> {
    let payload = postcard::to_allocvec(snapshot)?;
    let payload_hash = *blake3::hash(&payload).as_bytes();
    let mut header = Vec::with_capacity(HEADER_LEN);
    header.extend_from_slice(SNAPSHOT_MAGIC);
    header.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    header.push(SNAPSHOT_CODEC_POSTCARD);
    header.push(SNAPSHOT_COMPRESSION_NONE);
    header.extend_from_slice(&0_u32.to_le_bytes());
    header.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    header.extend_from_slice(&payload_hash);
    debug_assert_eq!(header.len(), HEADER_LEN);
    writer.write_all(&header)?;
    writer.write_all(&payload)?;
    Ok(())
}

pub(super) fn read_snapshot<R: Read>(mut reader: R) -> Result<SnapshotV2> {
    let mut header = [0_u8; HEADER_LEN];
    reader.read_exact(&mut header)?;
    if &header[0..8] != SNAPSHOT_MAGIC {
        return Err(Error::SnapshotValidation("invalid magic"));
    }
    let version = u16::from_le_bytes([header[8], header[9]]);
    if version != SNAPSHOT_VERSION {
        return Err(Error::SnapshotVersionUnsupported(version));
    }
    if header[10] != SNAPSHOT_CODEC_POSTCARD || header[11] != SNAPSHOT_COMPRESSION_NONE {
        return Err(Error::SnapshotValidation(
            "unsupported codec or compression",
        ));
    }
    let payload_len = u64::from_le_bytes(
        header[16..24]
            .try_into()
            .map_err(|_| Error::SnapshotValidation("invalid payload length"))?,
    ) as usize;
    let expected_hash: [u8; 32] = header[24..56]
        .try_into()
        .map_err(|_| Error::SnapshotValidation("invalid hash"))?;
    let mut payload = vec![0_u8; payload_len];
    reader.read_exact(&mut payload)?;
    if *blake3::hash(&payload).as_bytes() != expected_hash {
        return Err(Error::SnapshotHashMismatch);
    }
    Ok(postcard::from_bytes(&payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EngineConfig;
    use crate::snapshot::SnapshotMetadataV2;
    use crate::state_hash::{hash_canonical_state, CanonicalStateV2};

    fn minimal_snapshot() -> SnapshotV2 {
        let state = CanonicalStateV2 {
            config: EngineConfig::default(),
            currencies: Vec::new(),
            accounts: Vec::new(),
            books: Vec::new(),
            instruments: Vec::new(),
            positions: Vec::new(),
            lots: Vec::new(),
            marks: Vec::new(),
            fx_rates: Vec::new(),
            seen_events: Vec::new(),
            event_log: Vec::new(),
            last_seq: 0,
        };
        let state_hash = hash_canonical_state(&state);
        SnapshotV2 {
            metadata: SnapshotMetadataV2 {
                last_applied_event_seq: 0,
                state_hash,
                producer: "test".to_string(),
                build_version: "0.0.0".to_string(),
                fixture_identifier: Some("minimal".to_string()),
                user_notes: None,
            },
            state,
        }
    }

    #[test]
    fn writes_v2_header_and_payload_hash() {
        let snapshot = minimal_snapshot();
        let mut bytes = Vec::new();
        write_snapshot(&snapshot, &mut bytes).unwrap();

        assert_eq!(&bytes[0..8], SNAPSHOT_MAGIC);
        assert_eq!(u16::from_le_bytes([bytes[8], bytes[9]]), SNAPSHOT_VERSION);
        assert_eq!(bytes[10], SNAPSHOT_CODEC_POSTCARD);
        assert_eq!(bytes[11], SNAPSHOT_COMPRESSION_NONE);
        assert_eq!(&bytes[12..16], &[0, 0, 0, 0]);

        let payload_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        assert_eq!(payload_len, bytes.len() - HEADER_LEN);
        assert_eq!(
            &bytes[24..56],
            blake3::hash(&bytes[HEADER_LEN..]).as_bytes()
        );
        assert_eq!(read_snapshot(bytes.as_slice()).unwrap(), snapshot);
    }

    #[test]
    fn rejects_unsupported_snapshot_version_before_payload_decode() {
        let snapshot = minimal_snapshot();
        let mut bytes = Vec::new();
        write_snapshot(&snapshot, &mut bytes).unwrap();
        bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());

        assert_eq!(
            read_snapshot(bytes.as_slice()),
            Err(Error::SnapshotVersionUnsupported(1))
        );
    }
}
