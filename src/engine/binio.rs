use crate::{Result, TsinkError};

pub(crate) const FILE_FLAG_ZSTD_BODY: u16 = 0b0000_0001;
const FILE_ZSTD_ORIGINAL_LEN_PREFIX_BYTES: usize = 4;
const FILE_ZSTD_LEVEL_FAST: i32 = 1;

pub(crate) fn checksum32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

pub(crate) fn append_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

pub(crate) fn append_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn append_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn append_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn append_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn read_u8(bytes: &[u8], pos: &mut usize) -> Result<u8> {
    let byte = *bytes.get(*pos).ok_or_else(|| {
        TsinkError::DataCorruption("payload truncated while reading u8".to_string())
    })?;
    *pos = pos.saturating_add(1);
    Ok(byte)
}

pub(crate) fn read_u8_at(bytes: &[u8], offset: usize) -> Result<u8> {
    bytes.get(offset).copied().ok_or_else(|| {
        TsinkError::DataCorruption(format!("payload truncated while reading u8 at {offset}"))
    })
}

pub(crate) fn read_u16(bytes: &[u8], pos: &mut usize) -> Result<u16> {
    let mut raw = [0u8; 2];
    raw.copy_from_slice(read_bytes(bytes, pos, 2)?);
    Ok(u16::from_le_bytes(raw))
}

pub(crate) fn read_u32(bytes: &[u8], pos: &mut usize) -> Result<u32> {
    let mut raw = [0u8; 4];
    raw.copy_from_slice(read_bytes(bytes, pos, 4)?);
    Ok(u32::from_le_bytes(raw))
}

pub(crate) fn read_u64(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(read_bytes(bytes, pos, 8)?);
    Ok(u64::from_le_bytes(raw))
}

pub(crate) fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = offset.checked_add(4).ok_or_else(|| {
        TsinkError::DataCorruption("offset overflow while reading u32".to_string())
    })?;
    let slice = bytes.get(offset..end).ok_or_else(|| {
        TsinkError::DataCorruption(format!("payload truncated while reading u32 at {offset}"))
    })?;
    let mut raw = [0u8; 4];
    raw.copy_from_slice(slice);
    Ok(u32::from_le_bytes(raw))
}

pub(crate) fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        TsinkError::DataCorruption("offset overflow while reading u64".to_string())
    })?;
    let slice = bytes.get(offset..end).ok_or_else(|| {
        TsinkError::DataCorruption(format!("payload truncated while reading u64 at {offset}"))
    })?;
    let mut raw = [0u8; 8];
    raw.copy_from_slice(slice);
    Ok(u64::from_le_bytes(raw))
}

pub(crate) fn write_u64_at(bytes: &mut [u8], offset: usize, value: u64) -> Result<()> {
    let end = offset.checked_add(8).ok_or_else(|| {
        TsinkError::DataCorruption("offset overflow while writing u64".to_string())
    })?;
    let target = bytes.get_mut(offset..end).ok_or_else(|| {
        TsinkError::DataCorruption(format!("payload truncated while writing u64 at {offset}"))
    })?;
    target.copy_from_slice(&value.to_le_bytes());
    Ok(())
}

pub(crate) fn write_u32_at(bytes: &mut [u8], offset: usize, value: u32) -> Result<()> {
    let end = offset.checked_add(4).ok_or_else(|| {
        TsinkError::DataCorruption("offset overflow while writing u32".to_string())
    })?;
    let target = bytes.get_mut(offset..end).ok_or_else(|| {
        TsinkError::DataCorruption(format!("payload truncated while writing u32 at {offset}"))
    })?;
    target.copy_from_slice(&value.to_le_bytes());
    Ok(())
}

pub(crate) fn read_i64(bytes: &[u8], pos: &mut usize) -> Result<i64> {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(read_bytes(bytes, pos, 8)?);
    Ok(i64::from_le_bytes(raw))
}

pub(crate) fn read_array<const N: usize>(bytes: &[u8], pos: &mut usize) -> Result<[u8; N]> {
    let mut raw = [0u8; N];
    raw.copy_from_slice(read_bytes(bytes, pos, N)?);
    Ok(raw)
}

pub(crate) fn read_bytes<'a>(bytes: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = pos.saturating_add(len);
    if end > bytes.len() {
        return Err(TsinkError::DataCorruption(format!(
            "payload truncated: need {} bytes, have {}",
            len,
            bytes.len().saturating_sub(*pos)
        )));
    }

    let out = &bytes[*pos..end];
    *pos = end;
    Ok(out)
}

pub(crate) fn encode_optional_zstd_framed_file(logical_bytes: &[u8]) -> Result<Vec<u8>> {
    if logical_bytes.len() < 8 {
        return Err(TsinkError::InvalidConfiguration(
            "framed file is too short to compress".to_string(),
        ));
    }

    let original_flags = u16::from_le_bytes([logical_bytes[6], logical_bytes[7]]);
    let body = &logical_bytes[8..];
    if body.is_empty() {
        return Ok(logical_bytes.to_vec());
    }

    let compressed = zstd::bulk::compress(body, FILE_ZSTD_LEVEL_FAST).map_err(|err| {
        TsinkError::Compression(format!("zstd compress framed file failed: {err}"))
    })?;
    let original_len = u32::try_from(body.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("framed file body exceeds u32 length".to_string())
    })?;

    if compressed
        .len()
        .saturating_add(FILE_ZSTD_ORIGINAL_LEN_PREFIX_BYTES)
        >= body.len()
    {
        return Ok(logical_bytes.to_vec());
    }

    let mut out = Vec::with_capacity(
        8usize
            .saturating_add(FILE_ZSTD_ORIGINAL_LEN_PREFIX_BYTES)
            .saturating_add(compressed.len()),
    );
    out.extend_from_slice(&logical_bytes[..6]);
    append_u16(&mut out, original_flags | FILE_FLAG_ZSTD_BODY);
    append_u32(&mut out, original_len);
    out.extend_from_slice(&compressed);
    Ok(out)
}

pub(crate) fn decode_optional_zstd_framed_file(
    bytes: &[u8],
    expected_magic: [u8; 4],
    expected_version: u16,
    file_name: &str,
) -> Result<Vec<u8>> {
    if bytes.len() < 8 {
        return Err(TsinkError::DataCorruption(format!(
            "{file_name} is too short"
        )));
    }

    let mut pos = 0usize;
    let magic = read_array::<4>(bytes, &mut pos)?;
    if magic != expected_magic {
        return Err(TsinkError::DataCorruption(format!(
            "{file_name} magic mismatch"
        )));
    }

    let version = read_u16(bytes, &mut pos)?;
    if version != expected_version {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported {file_name} version {version}"
        )));
    }

    let flags = read_u16(bytes, &mut pos)?;

    let body = if flags & FILE_FLAG_ZSTD_BODY != 0 {
        let expected_len = usize::try_from(read_u32(bytes, &mut pos)?).unwrap_or(usize::MAX);
        let compressed = &bytes[pos..];
        let decoded = zstd::bulk::decompress(compressed, expected_len).map_err(|err| {
            TsinkError::Compression(format!("zstd decompress {file_name} failed: {err}"))
        })?;
        if decoded.len() != expected_len {
            return Err(TsinkError::DataCorruption(format!(
                "{file_name} decompressed length mismatch: expected {expected_len}, got {}",
                decoded.len()
            )));
        }
        decoded
    } else {
        bytes[pos..].to_vec()
    };

    let mut logical = Vec::with_capacity(8usize.saturating_add(body.len()));
    logical.extend_from_slice(&expected_magic);
    append_u16(&mut logical, expected_version);
    append_u16(&mut logical, flags & !FILE_FLAG_ZSTD_BODY);
    logical.extend_from_slice(&body);
    Ok(logical)
}
