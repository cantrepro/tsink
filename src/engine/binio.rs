use crate::{Result, TsinkError};

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
