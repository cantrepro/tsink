//! Bit stream implementation for Gorilla compression.

use bytes::{BufMut, BytesMut};
use std::borrow::Cow;
use std::io::{self, Read};

/// A stream of bits for writing.
pub struct BitStreamWriter {
    stream: BytesMut,
    count: u8, // How many bits are valid in current byte
}

impl Default for BitStreamWriter {
    fn default() -> Self {
        Self {
            stream: BytesMut::new(),
            count: 0,
        }
    }
}

impl BitStreamWriter {
    /// Creates a new BitStreamWriter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new BitStreamWriter with specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            stream: BytesMut::with_capacity(capacity),
            count: 0,
        }
    }

    /// Writes a single bit.
    pub fn write_bit(&mut self, bit: bool) {
        if self.count == 0 {
            self.stream.put_u8(0);
            self.count = 8;
        }

        let i = self.stream.len() - 1;
        if bit {
            self.stream[i] |= 1 << (self.count - 1);
        }

        self.count -= 1;
    }

    /// Writes a byte.
    pub fn write_byte(&mut self, byte: u8) {
        if self.count == 0 {
            self.stream.put_u8(byte);
            return;
        }

        let i = self.stream.len() - 1;

        // Fill up current byte with count bits from byte
        self.stream[i] |= byte >> (8 - self.count);

        // Add new byte with remaining bits
        self.stream.put_u8(byte << self.count);
    }

    /// Writes multiple bits.
    pub fn write_bits(&mut self, mut value: u64, mut nbits: usize) {
        if nbits == 0 {
            return;
        }
        assert!(nbits <= 64, "nbits must be <= 64");

        value <<= 64 - nbits;

        while nbits >= 8 {
            let byte = (value >> 56) as u8;
            self.write_byte(byte);
            value <<= 8;
            nbits -= 8;
        }

        while nbits > 0 {
            self.write_bit((value >> 63) == 1);
            value <<= 1;
            nbits -= 1;
        }
    }

    /// Returns the bytes written so far.
    pub fn bytes(&self) -> &[u8] {
        &self.stream
    }

    /// Consumes the writer and returns the bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.stream.to_vec()
    }

    /// Resets the buffer to be empty.
    pub fn reset(&mut self) {
        self.stream.clear();
        self.count = 0;
    }
}

/// A stream of bits for reading.
pub struct BitStreamReader<'a> {
    stream: Cow<'a, [u8]>,
    stream_offset: usize,
    buffer: u64,
    valid: u8, // Number of bits valid to read from left in buffer
}

impl<'a> BitStreamReader<'a> {
    /// Creates a new BitStreamReader from bytes.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self::from_cow(Cow::Owned(bytes))
    }

    /// Creates a new BitStreamReader borrowing an existing byte slice.
    pub fn from_slice(bytes: &'a [u8]) -> Self {
        Self::from_cow(Cow::Borrowed(bytes))
    }

    fn from_cow(stream: Cow<'a, [u8]>) -> Self {
        Self {
            stream,
            stream_offset: 0,
            buffer: 0,
            valid: 0,
        }
    }

    /// Reads a single bit.
    pub fn read_bit(&mut self) -> io::Result<bool> {
        if self.valid == 0 && !self.load_next_buffer(1) {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }
        self.read_bit_fast()
    }

    /// Fast version of read_bit that assumes buffer has data.
    pub fn read_bit_fast(&mut self) -> io::Result<bool> {
        if self.valid == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        self.valid -= 1;
        let bitmask = 1u64 << self.valid;
        Ok((self.buffer & bitmask) != 0)
    }

    /// Reads multiple bits.
    pub fn read_bits(&mut self, nbits: u8) -> io::Result<u64> {
        if nbits > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "nbits must be <= 64",
            ));
        }
        if nbits == 0 {
            return Ok(0);
        }
        if self.valid == 0 && !self.load_next_buffer(nbits) {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        if nbits <= self.valid {
            return self.read_bits_fast(nbits);
        }

        // Read all remaining valid bits and part from next buffer
        let bitmask = (1u64 << self.valid) - 1;
        let remaining_bits = nbits - self.valid;
        let v = (self.buffer & bitmask) << remaining_bits;
        self.valid = 0;

        if !self.load_next_buffer(remaining_bits) {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        let bitmask = (1u64 << remaining_bits) - 1;
        let v = v | ((self.buffer >> (self.valid - remaining_bits)) & bitmask);
        self.valid -= remaining_bits;

        Ok(v)
    }

    /// Fast version of read_bits that assumes buffer has enough data.
    pub fn read_bits_fast(&mut self, nbits: u8) -> io::Result<u64> {
        if nbits > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "nbits must be <= 64",
            ));
        }
        if nbits > self.valid {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        let bitmask = if nbits == 64 {
            u64::MAX
        } else {
            (1u64 << nbits) - 1
        };
        self.valid -= nbits;

        Ok((self.buffer >> self.valid) & bitmask)
    }

    /// Loads the next bytes from stream into buffer.
    fn load_next_buffer(&mut self, min_bits: u8) -> bool {
        if self.stream_offset >= self.stream.len() {
            return false;
        }

        // Try to load 8 bytes if possible (common case)
        if self.stream_offset + 8 <= self.stream.len() {
            self.buffer = u64::from_be_bytes([
                self.stream[self.stream_offset],
                self.stream[self.stream_offset + 1],
                self.stream[self.stream_offset + 2],
                self.stream[self.stream_offset + 3],
                self.stream[self.stream_offset + 4],
                self.stream[self.stream_offset + 5],
                self.stream[self.stream_offset + 6],
                self.stream[self.stream_offset + 7],
            ]);
            self.stream_offset += 8;
            self.valid = 64;
            return true;
        }

        // Load remaining bytes
        let nbytes = ((min_bits / 8) + 1) as usize;
        let nbytes = nbytes.min(self.stream.len() - self.stream_offset);

        let mut buffer = 0u64;
        for i in 0..nbytes {
            buffer |= (self.stream[self.stream_offset + i] as u64) << (8 * (nbytes - i - 1));
        }

        self.buffer = buffer;
        self.stream_offset += nbytes;
        self.valid = (nbytes * 8) as u8;

        true
    }
}

impl Read for BitStreamReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        for (i, byte) in buf.iter_mut().enumerate() {
            match self.read_bits(8) {
                Ok(v) => *byte = v as u8,
                Err(_) => return Ok(i),
            }
        }
        Ok(buf.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_stream_write_read() {
        let mut writer = BitStreamWriter::new();

        // Write some bits
        writer.write_bit(true);
        writer.write_bit(false);
        writer.write_bit(true);
        writer.write_bits(0b1010, 4);
        writer.write_byte(0xFF);

        // Read them back
        let bytes = writer.into_bytes();
        let mut reader = BitStreamReader::new(bytes);

        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert_eq!(reader.read_bits(4).unwrap(), 0b1010);
        assert_eq!(reader.read_bits(8).unwrap(), 0xFF);
    }

    #[test]
    fn test_write_bits_various_sizes() {
        let mut writer = BitStreamWriter::new();

        writer.write_bits(0b1, 1);
        writer.write_bits(0b101, 3);
        writer.write_bits(0b11111111, 8);
        writer.write_bits(0b101010101010, 12);

        let bytes = writer.into_bytes();
        let mut reader = BitStreamReader::new(bytes);

        assert_eq!(reader.read_bits(1).unwrap(), 0b1);
        assert_eq!(reader.read_bits(3).unwrap(), 0b101);
        assert_eq!(reader.read_bits(8).unwrap(), 0b11111111);
        assert_eq!(reader.read_bits(12).unwrap(), 0b101010101010);
    }

    #[test]
    fn test_read_bits_zero_and_invalid_width() {
        let mut reader = BitStreamReader::new(vec![0xAA]);
        assert_eq!(reader.read_bits(0).unwrap(), 0);
        assert!(reader.read_bits(65).is_err());
    }
}
