//! Gorilla time-series compression implementation.

use crate::bstream::{BitStreamReader, BitStreamWriter};
use crate::{DataPoint, Result, TsinkError};
use std::io::{self, Write};

/// Encoder for time-series data using Gorilla compression.
pub struct GorillaEncoder<W: Write> {
    writer: W,
    buf: BitStreamWriter,

    // Timestamp tracking
    t0: i64,
    t1: i64,
    t: i64,
    t_delta: u64,

    // Value tracking
    v: f64,
    leading: u8,
    trailing: u8,
}

impl<W: Write> GorillaEncoder<W> {
    /// Creates a new GorillaEncoder.
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            buf: BitStreamWriter::with_capacity(4096),
            t0: 0,
            t1: 0,
            t: 0,
            t_delta: 0,
            v: 0.0,
            leading: 0,
            trailing: 0,
        }
    }

    /// Encodes a data point.
    pub fn encode_point(&mut self, point: &DataPoint) -> Result<()> {
        match (self.t0, self.t1) {
            (0, _) => {
                // First point - write timestamp and value directly
                self.write_varint(point.timestamp)?;
                self.buf.write_bits(point.value.to_bits(), 64);
                self.t0 = point.timestamp;
            }
            (_, 0) => {
                // Second point - write delta of timestamp
                let t_delta = (point.timestamp - self.t0) as u64;
                self.write_uvarint(t_delta)?;
                self.write_value_delta(point.value);
                self.t1 = point.timestamp;
                self.t_delta = t_delta;
            }
            _ => {
                // Subsequent points - write delta-of-delta
                let t_delta = (point.timestamp - self.t) as u64;
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

                self.write_value_delta(point.value);
                self.t_delta = t_delta;
            }
        }

        self.t = point.timestamp;
        self.v = point.value;
        Ok(())
    }

    /// Writes value delta using XOR compression.
    fn write_value_delta(&mut self, value: f64) {
        let v_delta = value.to_bits() ^ self.v.to_bits();

        if v_delta == 0 {
            self.buf.write_bit(false);
            return;
        }

        self.buf.write_bit(true);

        let leading = v_delta.leading_zeros() as u8;
        let trailing = v_delta.trailing_zeros() as u8;

        // Clamp leading zeros to avoid overflow
        let leading = leading.min(31);

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

            // Handle edge case where all 64 bits are significant
            let mut sigbits = 64 - leading - trailing;
            if sigbits == 64 {
                sigbits = 0; // Encode as 0, decode as 64
            }

            self.buf.write_bits(sigbits as u64, 6);
            let actual_sigbits = if sigbits == 0 { 64 } else { sigbits };
            self.buf
                .write_bits(v_delta >> trailing, actual_sigbits as usize);
        }
    }

    /// Flushes the buffered data to the writer.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.write_all(self.buf.bytes())?;
        self.writer.flush()?;

        // Reset state
        self.buf.reset();
        self.t0 = 0;
        self.t1 = 0;
        self.t = 0;
        self.t_delta = 0;
        self.v = 0.0;
        self.leading = 0;
        self.trailing = 0;

        Ok(())
    }

    /// Writes a variable-length signed integer.
    fn write_varint(&mut self, value: i64) -> Result<()> {
        let mut buf = [0u8; 10];
        let len = encode_varint(value, &mut buf);
        for byte in buf.iter().take(len).copied() {
            self.buf.write_byte(byte);
        }
        Ok(())
    }

    /// Writes a variable-length unsigned integer.
    fn write_uvarint(&mut self, value: u64) -> Result<()> {
        let mut buf = [0u8; 10];
        let len = encode_uvarint(value, &mut buf);
        for byte in buf.iter().take(len).copied() {
            self.buf.write_byte(byte);
        }
        Ok(())
    }
}

/// Decoder for time-series data using Gorilla compression.
pub struct GorillaDecoder {
    reader: BitStreamReader,
    num_read: u16,

    // Timestamp tracking
    t: i64,
    t_delta: u64,

    // Value tracking
    v: f64,
    leading: u8,
    trailing: u8,
}

impl GorillaDecoder {
    /// Creates a new GorillaDecoder from bytes.
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            reader: BitStreamReader::new(data),
            num_read: 0,
            t: 0,
            t_delta: 0,
            v: 0.0,
            leading: 0,
            trailing: 0,
        }
    }

    /// Decodes a data point.
    pub fn decode_point(&mut self) -> Result<DataPoint> {
        match self.num_read {
            0 => {
                // First point
                self.t = self.read_varint()?;
                let v_bits = self.reader.read_bits(64)?;
                self.v = f64::from_bits(v_bits);
                self.num_read += 1;
                Ok(DataPoint::new(self.t, self.v))
            }
            1 => {
                // Second point
                self.t_delta = self.read_uvarint()?;
                self.t += self.t_delta as i64;
                self.read_value()?;
                self.num_read += 1;
                Ok(DataPoint::new(self.t, self.v))
            }
            _ => {
                // Subsequent points - read delta-of-delta
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
                Ok(DataPoint::new(self.t, self.v))
            }
        }
    }

    /// Reads value using XOR decompression.
    fn read_value(&mut self) -> Result<()> {
        let bit = self
            .reader
            .read_bit_fast()
            .or_else(|_| self.reader.read_bit())?;

        if !bit {
            // Value unchanged
            return Ok(());
        }

        let bit = self
            .reader
            .read_bit_fast()
            .or_else(|_| self.reader.read_bit())?;

        if !bit {
            // Reuse leading/trailing
        } else {
            // Read new leading/trailing
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

            // 0 means 64 significant bits
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

        let v_bits = self.v.to_bits();
        let v_bits = v_bits ^ (bits << self.trailing);
        self.v = f64::from_bits(v_bits);

        Ok(())
    }

    /// Reads a variable-length signed integer.
    fn read_varint(&mut self) -> Result<i64> {
        let mut result = 0u64;
        let mut shift = 0;

        loop {
            let byte = self.reader.read_bits(8)? as u8;
            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }

        // Zigzag decode
        Ok(((result >> 1) as i64) ^ -((result & 1) as i64))
    }

    /// Reads a variable-length unsigned integer.
    fn read_uvarint(&mut self) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0;

        loop {
            let byte = self.reader.read_bits(8)? as u8;
            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }

        Ok(result)
    }
}

/// Encodes a signed integer as varint.
fn encode_varint(value: i64, buf: &mut [u8]) -> usize {
    // Zigzag encode
    let uvalue = ((value << 1) ^ (value >> 63)) as u64;
    encode_uvarint(uvalue, buf)
}

/// Encodes an unsigned integer as varint.
fn encode_uvarint(mut value: u64, buf: &mut [u8]) -> usize {
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
    fn test_gorilla_encode_decode() {
        let points = vec![
            DataPoint::new(1000, 1.0),
            DataPoint::new(1060, 1.1),
            DataPoint::new(1120, 1.2),
            DataPoint::new(1180, 1.15),
            DataPoint::new(1240, 1.25),
        ];

        // Encode
        let mut buf = Vec::new();
        let mut encoder = GorillaEncoder::new(&mut buf);
        for point in &points {
            encoder.encode_point(point).unwrap();
        }
        encoder.flush().unwrap();

        // Decode
        let mut decoder = GorillaDecoder::new(buf);
        for expected in &points {
            let decoded = decoder.decode_point().unwrap();
            assert_eq!(decoded.timestamp, expected.timestamp);
            assert!((decoded.value - expected.value).abs() < 1e-10);
        }
    }

    #[test]
    fn test_varint_encoding() {
        let mut buf = [0u8; 10];

        // Test positive number
        let len = encode_varint(300, &mut buf);
        assert!(len <= 10);

        // Test negative number
        let len = encode_varint(-300, &mut buf);
        assert!(len <= 10);

        // Test zero
        let len = encode_varint(0, &mut buf);
        assert_eq!(len, 1);
    }
}
