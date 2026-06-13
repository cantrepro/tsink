//! Point encoding used for on-disk metric streams.

use crate::bstream::{BitStreamReader, BitStreamWriter};
use crate::{DataPoint, Result, TsinkError, Value};
use std::borrow::Cow;
use std::io::{self, Write};

const TAG_F64: u8 = 0;
const TAG_I64: u8 = 1;
const TAG_U64: u8 = 2;
const TAG_BOOL: u8 = 3;
const TAG_BYTES: u8 = 4;
const TAG_STRING: u8 = 5;

/// On-disk encoding strategy for a metric stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MetricEncoding {
    /// Generic per-point typed encoding.
    Typed,
    /// Gorilla delta/XOR stream where values are f64 bit patterns.
    GorillaF64,
    /// Gorilla delta/XOR stream where values are i64 bit patterns.
    GorillaI64,
    /// Gorilla delta/XOR stream where values are u64 bit patterns.
    GorillaU64,
    /// Gorilla delta/XOR stream where values are bool values encoded as 0/1.
    GorillaBool,
}

impl MetricEncoding {
    /// Chooses an encoding for a metric stream.
    ///
    /// Gorilla is used only when every point has the same fixed-width scalar type.
    pub fn infer_from_points(points: &[DataPoint]) -> Self {
        let Some(first) = points.first() else {
            return MetricEncoding::Typed;
        };

        let Some(kind) = Self::from_value(&first.value) else {
            return MetricEncoding::Typed;
        };

        if points
            .iter()
            .all(|point| Self::from_value(&point.value) == Some(kind))
        {
            kind
        } else {
            MetricEncoding::Typed
        }
    }

    pub fn is_gorilla(self) -> bool {
        !matches!(self, MetricEncoding::Typed)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::F64(_) => Some(MetricEncoding::GorillaF64),
            Value::I64(_) => Some(MetricEncoding::GorillaI64),
            Value::U64(_) => Some(MetricEncoding::GorillaU64),
            Value::Bool(_) => Some(MetricEncoding::GorillaBool),
            Value::Bytes(_) | Value::String(_) => None,
        }
    }

    pub fn value_to_bits(self, value: &Value) -> Result<u64> {
        match (self, value) {
            (MetricEncoding::GorillaF64, Value::F64(v)) => Ok(v.to_bits()),
            (MetricEncoding::GorillaI64, Value::I64(v)) => Ok(*v as u64),
            (MetricEncoding::GorillaU64, Value::U64(v)) => Ok(*v),
            (MetricEncoding::GorillaBool, Value::Bool(v)) => Ok(*v as u64),
            (MetricEncoding::Typed, _) => Err(TsinkError::Other(
                "Typed encoding does not convert to a single u64 bitstream".to_string(),
            )),
            (_, actual) => Err(TsinkError::ValueTypeMismatch {
                expected: format!("{:?}", self),
                actual: actual.kind().to_string(),
            }),
        }
    }

    pub fn bits_to_value(self, bits: u64) -> Result<Value> {
        match self {
            MetricEncoding::GorillaF64 => Ok(Value::F64(f64::from_bits(bits))),
            MetricEncoding::GorillaI64 => Ok(Value::I64(bits as i64)),
            MetricEncoding::GorillaU64 => Ok(Value::U64(bits)),
            MetricEncoding::GorillaBool => match bits {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                other => Err(TsinkError::DataCorruption(format!(
                    "invalid bool bit payload {other}"
                ))),
            },
            MetricEncoding::Typed => Err(TsinkError::Other(
                "Typed encoding requires PointDecoder".to_string(),
            )),
        }
    }
}

/// Encoder for typed time-series points.
pub struct PointEncoder<W: Write> {
    writer: W,
}

impl<W: Write> PointEncoder<W> {
    /// Creates a new point encoder.
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Encodes one data point.
    pub fn encode_point(&mut self, point: &DataPoint) -> Result<()> {
        self.write_varint(point.timestamp)?;

        match &point.value {
            Value::F64(value) => {
                self.writer.write_all(&[TAG_F64])?;
                self.writer.write_all(&value.to_bits().to_le_bytes())?;
            }
            Value::I64(value) => {
                self.writer.write_all(&[TAG_I64])?;
                self.write_varint(*value)?;
            }
            Value::U64(value) => {
                self.writer.write_all(&[TAG_U64])?;
                self.write_uvarint(*value)?;
            }
            Value::Bool(value) => {
                self.writer.write_all(&[TAG_BOOL])?;
                self.writer.write_all(&[*value as u8])?;
            }
            Value::Bytes(value) => {
                self.writer.write_all(&[TAG_BYTES])?;
                self.write_uvarint(value.len() as u64)?;
                self.writer.write_all(value)?;
            }
            Value::String(value) => {
                self.writer.write_all(&[TAG_STRING])?;
                self.write_uvarint(value.len() as u64)?;
                self.writer.write_all(value.as_bytes())?;
            }
        }

        Ok(())
    }

    /// Flushes buffered bytes.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Writes a variable-length signed integer.
    pub fn write_varint(&mut self, value: i64) -> Result<()> {
        let mut buf = [0u8; 10];
        let len = encode_varint(value, &mut buf);
        self.writer.write_all(&buf[..len])?;
        Ok(())
    }

    /// Writes a variable-length unsigned integer.
    pub fn write_uvarint(&mut self, value: u64) -> Result<()> {
        let mut buf = [0u8; 10];
        let len = encode_uvarint(value, &mut buf);
        self.writer.write_all(&buf[..len])?;
        Ok(())
    }
}

/// Decoder for typed time-series points.
pub struct PointDecoder<'a> {
    data: Cow<'a, [u8]>,
    offset: usize,
}

impl<'a> PointDecoder<'a> {
    /// Creates a decoder from owned bytes.
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: Cow::Owned(data),
            offset: 0,
        }
    }

    /// Creates a decoder borrowing an existing byte slice.
    pub fn from_slice(data: &'a [u8]) -> Self {
        Self {
            data: Cow::Borrowed(data),
            offset: 0,
        }
    }

    /// Decodes one data point.
    pub fn decode_point(&mut self) -> Result<DataPoint> {
        let timestamp = self.read_varint()?;
        let tag = self.read_byte()?;

        let value = match tag {
            TAG_F64 => {
                let bytes = self.read_exact(8)?;
                let mut arr = [0u8; 8];
                arr.copy_from_slice(bytes);
                Value::F64(f64::from_bits(u64::from_le_bytes(arr)))
            }
            TAG_I64 => Value::I64(self.read_varint()?),
            TAG_U64 => Value::U64(self.read_uvarint()?),
            TAG_BOOL => {
                let raw = self.read_byte()?;
                match raw {
                    0 => Value::Bool(false),
                    1 => Value::Bool(true),
                    other => {
                        return Err(TsinkError::DataCorruption(format!(
                            "invalid bool byte {other}"
                        )));
                    }
                }
            }
            TAG_BYTES => {
                let len = self.read_uvarint()?;
                let len = usize::try_from(len).map_err(|_| {
                    TsinkError::DataCorruption("bytes payload length exceeds usize".to_string())
                })?;
                let bytes = self.read_exact(len)?;
                Value::Bytes(bytes.to_vec())
            }
            TAG_STRING => {
                let len = self.read_uvarint()?;
                let len = usize::try_from(len).map_err(|_| {
                    TsinkError::DataCorruption("string payload length exceeds usize".to_string())
                })?;
                let bytes = self.read_exact(len)?;
                let value = String::from_utf8(bytes.to_vec()).map_err(|e| {
                    TsinkError::DataCorruption(format!("invalid UTF-8 string payload: {e}"))
                })?;
                Value::String(value)
            }
            _ => {
                return Err(TsinkError::DataCorruption(format!(
                    "unknown value tag {tag}"
                )));
            }
        };

        Ok(DataPoint::new(timestamp, value))
    }

    fn read_byte(&mut self) -> Result<u8> {
        if self.offset >= self.data.len() {
            return Err(TsinkError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF while reading byte",
            )));
        }

        let byte = self.data[self.offset];
        self.offset += 1;
        Ok(byte)
    }

    fn read_exact(&mut self, len: usize) -> Result<&[u8]> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            TsinkError::DataCorruption("overflow while reading point payload".to_string())
        })?;
        if end > self.data.len() {
            return Err(TsinkError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF while reading payload",
            )));
        }

        let bytes = &self.data[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    /// Reads a variable-length signed integer.
    pub fn read_varint(&mut self) -> Result<i64> {
        let result = self.read_uvarint()?;
        Ok(((result >> 1) as i64) ^ -((result & 1) as i64))
    }

    /// Reads a variable-length unsigned integer.
    pub fn read_uvarint(&mut self) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;

        for i in 0..10 {
            let byte = self.read_byte()?;

            if byte & 0x80 == 0 {
                if i == 9 && byte > 1 {
                    return Err(TsinkError::DataCorruption(
                        "uvarint overflow while decoding point".to_string(),
                    ));
                }
                result |= (byte as u64) << shift;
                return Ok(result);
            }

            result |= ((byte & 0x7F) as u64) << shift;
            shift += 7;
        }

        Err(TsinkError::DataCorruption(
            "uvarint overflow while decoding point".to_string(),
        ))
    }
}

/// Gorilla encoder for timestamp/value-bit streams.
pub struct Gorilla64Encoder<W: Write> {
    writer: W,
    buf: BitStreamWriter,

    num_points: u64,

    t: i64,
    t_delta: u64,

    v: u64,
    leading: u8,
    trailing: u8,
}

impl<W: Write> Gorilla64Encoder<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            buf: BitStreamWriter::with_capacity(4096),
            num_points: 0,
            t: 0,
            t_delta: 0,
            v: 0,
            leading: 0xff,
            trailing: 0,
        }
    }

    pub fn encode_point(&mut self, timestamp: i64, value_bits: u64) -> Result<()> {
        match self.num_points {
            0 => {
                self.write_varint(timestamp)?;
                self.buf.write_bits(value_bits, 64);
                self.t = timestamp;
            }
            1 => {
                let delta = timestamp.checked_sub(self.t).ok_or_else(|| {
                    TsinkError::Compression(
                        "timestamps must be non-decreasing for Gorilla encoding".to_string(),
                    )
                })?;
                if delta < 0 {
                    return Err(TsinkError::Compression(
                        "timestamps must be non-decreasing for Gorilla encoding".to_string(),
                    ));
                }
                let t_delta = delta as u64;
                self.write_uvarint(t_delta)?;
                self.write_value_delta(value_bits);
                self.t_delta = t_delta;
                self.t = timestamp;
            }
            _ => {
                let delta = timestamp.checked_sub(self.t).ok_or_else(|| {
                    TsinkError::Compression(
                        "timestamps must be non-decreasing for Gorilla encoding".to_string(),
                    )
                })?;
                if delta < 0 {
                    return Err(TsinkError::Compression(
                        "timestamps must be non-decreasing for Gorilla encoding".to_string(),
                    ));
                }

                let t_delta = delta as u64;
                let delta_of_delta = t_delta as i64 - self.t_delta as i64;

                match delta_of_delta {
                    0 => self.buf.write_bit(false),
                    -63..=64 => {
                        self.buf.write_bits(0b10, 2);
                        self.buf.write_bits(delta_of_delta as u64, 7);
                    }
                    -255..=256 => {
                        self.buf.write_bits(0b110, 3);
                        self.buf.write_bits(delta_of_delta as u64, 9);
                    }
                    -2047..=2048 => {
                        self.buf.write_bits(0b1110, 4);
                        self.buf.write_bits(delta_of_delta as u64, 12);
                    }
                    _ => {
                        self.buf.write_bits(0b1111, 4);
                        self.buf.write_bits(delta_of_delta as u64, 64);
                    }
                }

                self.write_value_delta(value_bits);
                self.t_delta = t_delta;
                self.t = timestamp;
            }
        }

        self.v = value_bits;
        self.num_points = self.num_points.saturating_add(1);
        Ok(())
    }

    fn write_value_delta(&mut self, value_bits: u64) {
        let v_delta = value_bits ^ self.v;

        if v_delta == 0 {
            self.buf.write_bit(false);
            return;
        }

        self.buf.write_bit(true);

        let leading = (v_delta.leading_zeros() as u8).min(31);
        let trailing = v_delta.trailing_zeros() as u8;

        if self.leading != 0xff && leading >= self.leading && trailing >= self.trailing {
            self.buf.write_bit(false);
            let significant_bits = 64 - self.leading - self.trailing;
            self.buf
                .write_bits(v_delta >> self.trailing, significant_bits as usize);
        } else {
            self.leading = leading;
            self.trailing = trailing;

            self.buf.write_bit(true);
            self.buf.write_bits(leading as u64, 5);

            let mut sigbits = 64 - leading - trailing;
            if sigbits == 64 {
                sigbits = 0;
            }

            self.buf.write_bits(sigbits as u64, 6);
            let actual_sigbits = if sigbits == 0 { 64 } else { sigbits };
            self.buf
                .write_bits(v_delta >> trailing, actual_sigbits as usize);
        }
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.write_all(self.buf.bytes())?;
        self.writer.flush()?;

        self.buf.reset();
        self.num_points = 0;
        self.t = 0;
        self.t_delta = 0;
        self.v = 0;
        self.leading = 0xff;
        self.trailing = 0;

        Ok(())
    }

    fn write_varint(&mut self, value: i64) -> Result<()> {
        let mut buf = [0u8; 10];
        let len = encode_varint(value, &mut buf);
        for byte in buf.iter().take(len).copied() {
            self.buf.write_byte(byte);
        }
        Ok(())
    }

    fn write_uvarint(&mut self, value: u64) -> Result<()> {
        let mut buf = [0u8; 10];
        let len = encode_uvarint(value, &mut buf);
        for byte in buf.iter().take(len).copied() {
            self.buf.write_byte(byte);
        }
        Ok(())
    }
}

/// Gorilla decoder for timestamp/value-bit streams.
pub struct Gorilla64Decoder<'a> {
    reader: BitStreamReader<'a>,
    num_read: u16,

    t: i64,
    t_delta: u64,

    v: u64,
    leading: u8,
    trailing: u8,
}

impl<'a> Gorilla64Decoder<'a> {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            reader: BitStreamReader::new(data),
            num_read: 0,
            t: 0,
            t_delta: 0,
            v: 0,
            leading: 0,
            trailing: 0,
        }
    }

    pub fn from_slice(data: &'a [u8]) -> Self {
        Self {
            reader: BitStreamReader::from_slice(data),
            num_read: 0,
            t: 0,
            t_delta: 0,
            v: 0,
            leading: 0,
            trailing: 0,
        }
    }

    pub fn decode_point(&mut self) -> Result<(i64, u64)> {
        match self.num_read {
            0 => {
                self.t = self.read_varint()?;
                self.v = self.reader.read_bits(64)?;
                self.num_read += 1;
                Ok((self.t, self.v))
            }
            1 => {
                self.t_delta = self.read_uvarint()?;
                self.t += self.t_delta as i64;
                self.read_value()?;
                self.num_read += 1;
                Ok((self.t, self.v))
            }
            _ => {
                let mut delimiter = 0u8;
                for _ in 0..4 {
                    delimiter <<= 1;
                    let bit = self
                        .reader
                        .read_bit_fast()
                        .or_else(|_| self.reader.read_bit())?;
                    if !bit {
                        break;
                    }
                    delimiter |= 1;
                }

                let delta_of_delta = match delimiter {
                    0x00 => 0i64,
                    0x02 => {
                        let bits = self
                            .reader
                            .read_bits_fast(7)
                            .or_else(|_| self.reader.read_bits(7))?;
                        if bits > (1 << 6) {
                            bits as i64 - (1 << 7)
                        } else {
                            bits as i64
                        }
                    }
                    0x06 => {
                        let bits = self
                            .reader
                            .read_bits_fast(9)
                            .or_else(|_| self.reader.read_bits(9))?;
                        if bits > (1 << 8) {
                            bits as i64 - (1 << 9)
                        } else {
                            bits as i64
                        }
                    }
                    0x0e => {
                        let bits = self
                            .reader
                            .read_bits_fast(12)
                            .or_else(|_| self.reader.read_bits(12))?;
                        if bits > (1 << 11) {
                            bits as i64 - (1 << 12)
                        } else {
                            bits as i64
                        }
                    }
                    0x0f => self.reader.read_bits(64)? as i64,
                    _ => {
                        return Err(TsinkError::Other(format!(
                            "Unknown delimiter: {}",
                            delimiter
                        )));
                    }
                };

                self.t_delta = (self.t_delta as i64 + delta_of_delta) as u64;
                self.t += self.t_delta as i64;
                self.read_value()?;
                Ok((self.t, self.v))
            }
        }
    }

    fn read_value(&mut self) -> Result<()> {
        let bit = self
            .reader
            .read_bit_fast()
            .or_else(|_| self.reader.read_bit())?;

        if !bit {
            return Ok(());
        }

        let bit = self
            .reader
            .read_bit_fast()
            .or_else(|_| self.reader.read_bit())?;

        if bit {
            let bits = self
                .reader
                .read_bits_fast(5)
                .or_else(|_| self.reader.read_bits(5))?;
            self.leading = bits as u8;

            let bits = self
                .reader
                .read_bits_fast(6)
                .or_else(|_| self.reader.read_bits(6))?;
            let mut mbits = bits as u8;

            if mbits == 0 {
                mbits = 64;
            }

            self.trailing = 64 - self.leading - mbits;
        }

        let mbits = 64 - self.leading - self.trailing;
        let bits = self
            .reader
            .read_bits_fast(mbits)
            .or_else(|_| self.reader.read_bits(mbits))?;

        self.v ^= bits << self.trailing;

        Ok(())
    }

    fn read_varint(&mut self) -> Result<i64> {
        let result = self.read_uvarint()?;
        Ok(((result >> 1) as i64) ^ -((result & 1) as i64))
    }

    fn read_uvarint(&mut self) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;

        for i in 0..10 {
            let byte = self.reader.read_bits(8)? as u8;

            if byte & 0x80 == 0 {
                if i == 9 && byte > 1 {
                    return Err(TsinkError::DataCorruption(
                        "uvarint overflow while decoding timestamp/value".to_string(),
                    ));
                }
                result |= (byte as u64) << shift;
                return Ok(result);
            }

            result |= ((byte & 0x7F) as u64) << shift;
            shift += 7;
        }

        Err(TsinkError::DataCorruption(
            "uvarint overflow while decoding timestamp/value".to_string(),
        ))
    }
}

/// Encodes a signed integer as varint.
pub fn encode_varint(value: i64, buf: &mut [u8]) -> usize {
    let uvalue = ((value << 1) ^ (value >> 63)) as u64;
    encode_uvarint(uvalue, buf)
}

/// Encodes an unsigned integer as varint.
pub fn encode_uvarint(mut value: u64, buf: &mut [u8]) -> usize {
    let mut i = 0;
    while value >= 0x80 {
        buf[i] = (value as u8) | 0x80;
        value >>= 7;
        i += 1;
    }
    buf[i] = value as u8;
    i + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_encoder_roundtrips_all_value_kinds() {
        let points = vec![
            DataPoint::new(1000, 1.0f64),
            DataPoint::new(1060, -5i64),
            DataPoint::new(1120, 10u64),
            DataPoint::new(1180, true),
            DataPoint::new(1240, Value::Bytes(vec![1, 2, 3])),
            DataPoint::new(1300, "hello"),
        ];

        let mut buf = Vec::new();
        let mut encoder = PointEncoder::new(&mut buf);
        for point in &points {
            encoder.encode_point(point).unwrap();
        }
        encoder.flush().unwrap();

        let mut decoder = PointDecoder::from_slice(&buf);
        for expected in &points {
            let decoded = decoder.decode_point().unwrap();
            assert_eq!(&decoded, expected);
        }
    }

    #[test]
    fn gorilla64_roundtrips_f64_bits() {
        let points = [
            (1000, 1.0f64.to_bits()),
            (1060, 1.1f64.to_bits()),
            (1120, 1.2f64.to_bits()),
            (1180, 1.15f64.to_bits()),
            (1240, 1.25f64.to_bits()),
        ];

        let mut buf = Vec::new();
        let mut encoder = Gorilla64Encoder::new(&mut buf);
        for (ts, bits) in points {
            encoder.encode_point(ts, bits).unwrap();
        }
        encoder.flush().unwrap();

        let mut decoder = Gorilla64Decoder::from_slice(&buf);
        for expected in points {
            let decoded = decoder.decode_point().unwrap();
            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn gorilla64_roundtrips_i64_u64_and_bool_bits() {
        let streams = vec![
            vec![(10, (-1i64) as u64), (20, (123i64) as u64), (30, 0u64)],
            vec![(10, 1u64), (20, 42u64), (30, u64::MAX - 5)],
            vec![(10, 0u64), (20, 1u64), (30, 1u64), (40, 0u64)],
        ];

        for points in streams {
            let mut buf = Vec::new();
            let mut encoder = Gorilla64Encoder::new(&mut buf);
            for (ts, bits) in points.iter().copied() {
                encoder.encode_point(ts, bits).unwrap();
            }
            encoder.flush().unwrap();

            let mut decoder = Gorilla64Decoder::from_slice(&buf);
            for expected in points {
                assert_eq!(decoder.decode_point().unwrap(), expected);
            }
        }
    }

    #[test]
    fn gorilla64_rejects_decreasing_timestamps() {
        let mut buf = Vec::new();
        let mut encoder = Gorilla64Encoder::new(&mut buf);
        encoder.encode_point(10, 1).unwrap();
        let err = encoder.encode_point(9, 2).unwrap_err();
        assert!(matches!(err, TsinkError::Compression(_)));
    }

    #[test]
    fn metric_encoding_infer_detects_homogeneous_and_mixed_streams() {
        assert_eq!(
            MetricEncoding::infer_from_points(&[
                DataPoint::new(1, 1.0f64),
                DataPoint::new(2, 2.0f64)
            ]),
            MetricEncoding::GorillaF64
        );
        assert_eq!(
            MetricEncoding::infer_from_points(&[DataPoint::new(1, -1i64), DataPoint::new(2, 2i64)]),
            MetricEncoding::GorillaI64
        );
        assert_eq!(
            MetricEncoding::infer_from_points(&[DataPoint::new(1, 1u64), DataPoint::new(2, 2u64)]),
            MetricEncoding::GorillaU64
        );
        assert_eq!(
            MetricEncoding::infer_from_points(&[DataPoint::new(1, false), DataPoint::new(2, true)]),
            MetricEncoding::GorillaBool
        );
        assert_eq!(
            MetricEncoding::infer_from_points(&[
                DataPoint::new(1, 1.0f64),
                DataPoint::new(2, 2i64)
            ]),
            MetricEncoding::Typed
        );
        assert_eq!(
            MetricEncoding::infer_from_points(&[DataPoint::new(1, "a"), DataPoint::new(2, "b")]),
            MetricEncoding::Typed
        );
    }

    #[test]
    fn metric_encoding_value_bit_roundtrips_and_rejects_invalid_bool_bits() {
        let cases = [
            (MetricEncoding::GorillaF64, Value::F64(12.5)),
            (MetricEncoding::GorillaI64, Value::I64(-7)),
            (MetricEncoding::GorillaU64, Value::U64(9)),
            (MetricEncoding::GorillaBool, Value::Bool(true)),
        ];

        for (encoding, value) in cases {
            let bits = encoding.value_to_bits(&value).unwrap();
            let decoded = encoding.bits_to_value(bits).unwrap();
            assert_eq!(decoded, value);
        }

        let err = MetricEncoding::GorillaBool.bits_to_value(2).unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(_)));
    }

    #[test]
    fn metric_encoding_rejects_value_type_mismatch() {
        let err = MetricEncoding::GorillaU64
            .value_to_bits(&Value::String("nope".to_string()))
            .unwrap_err();
        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
    }

    #[test]
    fn decoder_rejects_invalid_bool_byte() {
        let mut buf = Vec::new();
        let mut enc = PointEncoder::new(&mut buf);
        enc.write_varint(1).unwrap();
        buf.push(TAG_BOOL);
        buf.push(2);

        let mut decoder = PointDecoder::from_slice(&buf);
        let err = decoder.decode_point().unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(_)));
    }

    #[test]
    fn decoder_rejects_varint_overflow_without_panic() {
        let mut decoder = PointDecoder::from_slice(&[0x80; 11]);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| decoder.decode_point()));

        assert!(result.is_ok(), "decoder should return an error, not panic");
        assert!(matches!(
            result.unwrap(),
            Err(TsinkError::DataCorruption(_))
        ));
    }
}
