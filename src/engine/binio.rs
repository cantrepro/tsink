use std::borrow::Cow;
use std::io::Read;

use crate::{Result, TsinkError};

pub(crate) const FILE_FLAG_ZSTD_BODY: u16 = 0b0000_0001;
/// Last-resort format safety boundary for decoded registry and segment metadata files.
///
/// Resource profiles may impose a lower runtime memory limit. This ceiling exists so a corrupt
/// persisted length cannot request a multi-gigabyte allocation before those higher-level budgets
/// can inspect the file.
pub(crate) const MAX_DECODED_FRAMED_FILE_BYTES: usize = 256 * 1024 * 1024;
const FILE_ZSTD_ORIGINAL_LEN_PREFIX_BYTES: usize = 4;
const FILE_ZSTD_LEVEL_FAST: i32 = 1;
const ZSTD_DECODE_BUFFER_BYTES: usize = 16 * 1024;

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
    let end = pos.checked_add(len).ok_or_else(|| {
        TsinkError::DataCorruption("payload position overflow while reading bytes".to_string())
    })?;
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
    if logical_bytes.len() > MAX_DECODED_FRAMED_FILE_BYTES {
        return Err(TsinkError::InvalidConfiguration(format!(
            "framed file size {} exceeds the format safety limit {MAX_DECODED_FRAMED_FILE_BYTES}",
            logical_bytes.len()
        )));
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

    let stored_compressed_len = compressed
        .len()
        .checked_add(FILE_ZSTD_ORIGINAL_LEN_PREFIX_BYTES)
        .ok_or_else(|| {
            TsinkError::InvalidConfiguration("compressed framed file length overflow".to_string())
        })?;
    if stored_compressed_len >= body.len() {
        return Ok(logical_bytes.to_vec());
    }

    let output_len = 8usize.checked_add(stored_compressed_len).ok_or_else(|| {
        TsinkError::InvalidConfiguration("compressed framed file length overflow".to_string())
    })?;
    let mut out = Vec::with_capacity(output_len);
    out.extend_from_slice(&logical_bytes[..6]);
    append_u16(&mut out, original_flags | FILE_FLAG_ZSTD_BODY);
    append_u32(&mut out, original_len);
    out.extend_from_slice(&compressed);
    Ok(out)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decode_optional_zstd_framed_file<'a>(
    bytes: &'a [u8],
    expected_magic: [u8; 4],
    expected_version: u16,
    file_name: &str,
) -> Result<Cow<'a, [u8]>> {
    decode_optional_zstd_framed_file_with_limit(
        bytes,
        expected_magic,
        expected_version,
        file_name,
        MAX_DECODED_FRAMED_FILE_BYTES,
    )
}

pub(crate) fn decode_optional_zstd_framed_file_with_limit<'a>(
    bytes: &'a [u8],
    expected_magic: [u8; 4],
    expected_version: u16,
    file_name: &str,
    max_decoded_file_bytes: usize,
) -> Result<Cow<'a, [u8]>> {
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

    if flags & FILE_FLAG_ZSTD_BODY == 0 {
        if bytes.len() > max_decoded_file_bytes {
            return Err(decoded_size_limit_error(
                file_name,
                bytes.len(),
                max_decoded_file_bytes,
            ));
        }
        return Ok(Cow::Borrowed(bytes));
    }

    let expected_body_len = usize::try_from(read_u32(bytes, &mut pos)?).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "{file_name} decoded body length does not fit this platform"
        ))
    })?;
    let expected_file_len = 8usize.checked_add(expected_body_len).ok_or_else(|| {
        TsinkError::DataCorruption(format!("{file_name} decoded length overflow"))
    })?;
    if expected_file_len > max_decoded_file_bytes {
        return Err(decoded_size_limit_error(
            file_name,
            expected_file_len,
            max_decoded_file_bytes,
        ));
    }

    let initial_body_capacity = expected_body_len.min(ZSTD_DECODE_BUFFER_BYTES);
    let initial_file_capacity = 8usize.checked_add(initial_body_capacity).ok_or_else(|| {
        TsinkError::DataCorruption(format!("{file_name} initial decoded length overflow"))
    })?;
    let mut logical = Vec::new();
    logical
        .try_reserve_exact(initial_file_capacity)
        .map_err(|err| {
            TsinkError::Other(format!(
                "failed to reserve {initial_file_capacity} initial bytes while decoding {file_name}: {err}"
            ))
        })?;
    logical.extend_from_slice(&expected_magic);
    append_u16(&mut logical, expected_version);
    append_u16(&mut logical, flags & !FILE_FLAG_ZSTD_BODY);
    decompress_zstd_exact_into(&bytes[pos..], expected_body_len, file_name, &mut logical)?;
    debug_assert_eq!(logical.len(), expected_file_len);
    Ok(Cow::Owned(logical))
}

fn decoded_size_limit_error(file_name: &str, declared: usize, limit: usize) -> TsinkError {
    TsinkError::DataCorruption(format!(
        "{file_name} decoded size {declared} exceeds the format safety limit {limit}"
    ))
}

pub(crate) fn decompress_zstd_exact_bounded(
    compressed: &[u8],
    expected_len: usize,
    max_decoded_bytes: usize,
    context: &str,
) -> Result<Vec<u8>> {
    if expected_len > max_decoded_bytes {
        return Err(decoded_size_limit_error(
            context,
            expected_len,
            max_decoded_bytes,
        ));
    }

    let initial_capacity = expected_len.min(ZSTD_DECODE_BUFFER_BYTES);
    let mut decoded = Vec::new();
    decoded.try_reserve_exact(initial_capacity).map_err(|err| {
        TsinkError::Other(format!(
            "failed to reserve {initial_capacity} initial bytes while decoding {context}: {err}"
        ))
    })?;
    decompress_zstd_exact_into(compressed, expected_len, context, &mut decoded)?;
    Ok(decoded)
}

pub(crate) fn read_to_end_bounded(
    reader: &mut impl Read,
    max_bytes: usize,
    initial_size_hint: usize,
    context: &str,
) -> Result<Vec<u8>> {
    let initial_capacity = initial_size_hint
        .min(max_bytes)
        .min(ZSTD_DECODE_BUFFER_BYTES);
    let mut out = Vec::new();
    out.try_reserve_exact(initial_capacity).map_err(|err| {
        TsinkError::Other(format!(
            "failed to reserve {initial_capacity} initial bytes while reading {context}: {err}"
        ))
    })?;
    let mut buffer = [0u8; ZSTD_DECODE_BUFFER_BYTES];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(out);
        }
        let next_len = out.len().checked_add(read).ok_or_else(|| {
            TsinkError::DataCorruption(format!("{context} length overflow while reading"))
        })?;
        if next_len > max_bytes {
            return Err(TsinkError::DataCorruption(format!(
                "{context} exceeds the format safety limit {max_bytes} while reading"
            )));
        }
        let spare_capacity = out.capacity().saturating_sub(out.len());
        if spare_capacity < read {
            let additional = read - spare_capacity;
            out.try_reserve_exact(additional).map_err(|err| {
                TsinkError::Other(format!(
                    "failed to reserve {additional} more bytes while reading {context}: {err}"
                ))
            })?;
        }
        out.extend_from_slice(&buffer[..read]);
    }
}

fn decompress_zstd_exact_into(
    compressed: &[u8],
    expected_len: usize,
    file_name: &str,
    out: &mut Vec<u8>,
) -> Result<()> {
    let output_start = out.len();
    let mut decoder = zstd::stream::read::Decoder::new(compressed).map_err(|err| {
        TsinkError::Compression(format!("zstd decompress {file_name} failed: {err}"))
    })?;
    let mut buffer = [0u8; ZSTD_DECODE_BUFFER_BYTES];

    loop {
        let read = decoder.read(&mut buffer).map_err(|err| {
            TsinkError::Compression(format!("zstd decompress {file_name} failed: {err}"))
        })?;
        if read == 0 {
            break;
        }

        let decoded_len = out.len().checked_sub(output_start).ok_or_else(|| {
            TsinkError::DataCorruption(format!("{file_name} decoded length underflow"))
        })?;
        let next_len = decoded_len.checked_add(read).ok_or_else(|| {
            TsinkError::DataCorruption(format!("{file_name} decoded length overflow"))
        })?;
        if next_len > expected_len {
            return Err(TsinkError::DataCorruption(format!(
                "{file_name} decompressed length exceeds declared length {expected_len}"
            )));
        }
        let spare_capacity = out.capacity().saturating_sub(out.len());
        if spare_capacity < read {
            let additional = read - spare_capacity;
            out.try_reserve_exact(additional).map_err(|err| {
                TsinkError::Other(format!(
                    "failed to reserve {additional} more bytes while decoding {file_name}: {err}"
                ))
            })?;
        }
        out.extend_from_slice(&buffer[..read]);
    }

    let actual_len = out.len().checked_sub(output_start).ok_or_else(|| {
        TsinkError::DataCorruption(format!("{file_name} decoded length underflow"))
    })?;
    if actual_len != expected_len {
        return Err(TsinkError::DataCorruption(format!(
            "{file_name} decompressed length mismatch: expected {expected_len}, got {actual_len}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAGIC: [u8; 4] = *b"TST2";
    const VERSION: u16 = 2;

    fn compressed_frame(body: &[u8], declared_len: u32) -> Vec<u8> {
        let compressed = zstd::bulk::compress(body, 1).unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        append_u16(&mut bytes, VERSION);
        append_u16(&mut bytes, FILE_FLAG_ZSTD_BODY);
        append_u32(&mut bytes, declared_len);
        bytes.extend_from_slice(&compressed);
        bytes
    }

    #[test]
    fn bounded_framed_decode_accepts_exact_limit_and_rejects_n_plus_one() {
        let body = vec![7u8; 64];
        let encoded = compressed_frame(&body, body.len() as u32);
        let decoded = decode_optional_zstd_framed_file_with_limit(
            &encoded,
            MAGIC,
            VERSION,
            "test frame",
            8 + body.len(),
        )
        .unwrap();
        assert_eq!(&decoded[8..], body.as_slice());

        let err = decode_optional_zstd_framed_file_with_limit(
            &encoded,
            MAGIC,
            VERSION,
            "test frame",
            8 + body.len() - 1,
        )
        .unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("decoded size 72 exceeds the format safety limit 71")));
    }

    #[test]
    fn bounded_framed_decode_rejects_huge_declared_length_before_decompression() {
        let encoded = compressed_frame(&[1], u32::MAX);
        let err = decode_optional_zstd_framed_file_with_limit(
            &encoded,
            MAGIC,
            VERSION,
            "test frame",
            1024,
        )
        .unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("exceeds the format safety limit 1024")));
    }

    #[test]
    fn bounded_framed_decode_rejects_wrong_actual_length_and_corrupt_stream() {
        let short_declaration = compressed_frame(&[3u8; 32], 31);
        let err = decode_optional_zstd_framed_file_with_limit(
            &short_declaration,
            MAGIC,
            VERSION,
            "test frame",
            1024,
        )
        .unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("exceeds declared length 31")));

        let mut corrupt = compressed_frame(&[3u8; 32], 32);
        corrupt.truncate(corrupt.len() - 2);
        let err = decode_optional_zstd_framed_file_with_limit(
            &corrupt,
            MAGIC,
            VERSION,
            "test frame",
            1024,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::Compression(_) | TsinkError::DataCorruption(_)
        ));
    }

    #[test]
    fn uncompressed_framed_decode_borrows_and_obeys_the_same_limit() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        append_u16(&mut bytes, VERSION);
        append_u16(&mut bytes, 0);
        bytes.extend_from_slice(&[9u8; 8]);

        let decoded = decode_optional_zstd_framed_file_with_limit(
            &bytes,
            MAGIC,
            VERSION,
            "test frame",
            bytes.len(),
        )
        .unwrap();
        assert!(matches!(decoded, Cow::Borrowed(_)));

        assert!(decode_optional_zstd_framed_file_with_limit(
            &bytes,
            MAGIC,
            VERSION,
            "test frame",
            bytes.len() - 1,
        )
        .is_err());
    }

    #[test]
    fn read_bytes_rejects_position_arithmetic_overflow() {
        let mut pos = usize::MAX;
        let err = read_bytes(&[], &mut pos, 1).unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("position overflow")));
    }

    #[test]
    fn bounded_reader_accepts_n_and_rejects_n_plus_one_without_appending_excess() {
        let mut exact = std::io::Cursor::new(vec![1u8; 32]);
        assert_eq!(
            read_to_end_bounded(&mut exact, 32, usize::MAX, "test input")
                .unwrap()
                .len(),
            32
        );

        let mut oversized = std::io::Cursor::new(vec![1u8; 33]);
        let err = read_to_end_bounded(&mut oversized, 32, usize::MAX, "test input").unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(message)
            if message.contains("exceeds the format safety limit 32")));
    }
}
