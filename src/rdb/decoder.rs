use bytes::{BufMut, Bytes, BytesMut};
use chrono::DateTime;

use std::collections::HashMap;
use std::string::FromUtf8Error;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::store::{StoreValue, StoreValueBuilder};

#[derive(Debug)]
enum RdbValueType {
    StringEncoding,
}

impl RdbValueType {
    fn from_byte(byte: u8) -> Option<RdbValueType> {
        match format!("{:X}", byte).as_str() {
            "0" => Some(RdbValueType::StringEncoding),
            _ => None,
        }
    }
}

#[derive(Debug)]
enum RdbSection {
    METADATA,
    DATABASE,
    HEADER,
    END_OF_FILE,
}

impl RdbSection {
    fn from_byte(byte: u8) -> Option<RdbSection> {
        match format!("{:X}", byte).as_str() {
            "FA" => Some(RdbSection::METADATA),
            "FE" => Some(RdbSection::DATABASE),
            "FF" => Some(RdbSection::END_OF_FILE),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct RdbData {
    pub header: RdbDataHeader,
    pub metadata: Option<RdbDataMetadata>,
    pub databases: Option<RdbDataDatabases>,
    pub checksum: u64,
}

#[derive(Debug)]
pub struct RdbDataHeader {
    pub magic_string: String,
    pub version: String,
}

#[derive(Debug)]
pub struct RdbDataMetadata {
    redis_ver: Option<String>,
    redis_bits: Option<String>,
    ctime: Option<String>,
    used_mem: Option<String>,
}

impl RdbDataMetadata {
    fn new() -> Self {
        RdbDataMetadata {
            redis_ver: None,
            redis_bits: None,
            ctime: None,
            used_mem: None,
        }
    }

    fn set_key(&mut self, key: &str, value: &str) -> Result<(), RdbFileDecoderError> {
        match key {
            "redis-ver" => {
                self.redis_ver = Some(value.to_string());
                Ok(())
            }
            "redis-bits" => {
                self.redis_bits = Some(value.to_string());
                Ok(())
            }
            "ctime" => {
                self.ctime = Some(value.to_string());
                Ok(())
            }
            "used-mem" => {
                self.used_mem = Some(value.to_string());
                Ok(())
            }
            _ => Err(RdbFileDecoderError::InvalidMetadata(String::from(
                "Invalid metadata key",
            ))),
        }
    }
}

#[derive(Debug, Default)]
pub struct RdbDataDatabases {
    pub databases: HashMap<usize, RdbDatabase>,
}

impl RdbDataDatabases {
    fn add_database(&mut self, database: RdbDatabase) {
        self.databases.insert(database.index, database);
    }
}

#[derive(Debug)]
pub struct RdbDatabase {
    pub index: usize,
    number_of_keys: Size,
    number_of_expired_keys: Size,
    pub data: HashMap<String, StoreValue>,
}

impl RdbDatabase {
    fn new(index: usize, number_of_keys: Size, number_of_expired_keys: Size) -> Self {
        Self {
            index,
            number_of_keys,
            number_of_expired_keys,
            data: HashMap::new(),
        }
    }

    fn set(&mut self, key: &str, value: StoreValue) {
        self.data.insert(key.to_string(), value);
    }
}

#[derive(Debug)]
struct RdbDataBuilder {
    header: Option<RdbDataHeader>,
    metadata: Option<RdbDataMetadata>,
    databases: Option<RdbDataDatabases>,
    checksum: Option<u64>,
}

impl RdbDataBuilder {
    pub fn new() -> RdbDataBuilder {
        RdbDataBuilder {
            header: None,
            metadata: None,
            databases: None,
            checksum: None,
        }
    }

    pub fn with_header(&mut self, header: RdbDataHeader) {
        self.header = Some(header);
    }

    pub fn with_metadata(&mut self, metadata: RdbDataMetadata) {
        self.metadata = Some(metadata);
    }

    pub fn with_databases(&mut self, databases: RdbDataDatabases) {
        self.databases = Some(databases);
    }

    pub fn build(self) -> RdbData {
        RdbData {
            header: self.header.unwrap(),
            metadata: self.metadata,
            databases: self.databases,
            checksum: self.checksum.unwrap_or(0),
        }
    }
}

#[derive(Debug)]
pub enum RdbFileDecoderError {
    InvalidStringConversion(FromUtf8Error),
    InvalidArrayConversion(String),
    InvalidMetadata(String),
    InvalidSection(String),
    ReadFile(String),
    InvalidNumberConversion(String),
    InvalidSize,
    InvalidString,
    InvalidExpirationConversion(std::num::TryFromIntError),
    MissingDbIndex,
    EmptyBuffer,
}

impl std::fmt::Display for RdbFileDecoderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdbFileDecoderError::InvalidStringConversion(err) => {
                write!(f, "InvalidHeader Error: {}", err)
            }
            RdbFileDecoderError::InvalidMetadata(err) => {
                write!(f, "InvalidMetadata Error: {}", err)
            }
            RdbFileDecoderError::ReadFile(err) => {
                write!(f, "ReadFile Error: {}", err)
            }
            RdbFileDecoderError::InvalidSection(err) => {
                write!(f, "InvalidSection Error: {}", err)
            }
            RdbFileDecoderError::InvalidNumberConversion(err) => {
                write!(f, "InvalidNumberConversion Error: {}", err)
            }
            RdbFileDecoderError::InvalidSize => {
                write!(f, "InvalidSize Error")
            }
            RdbFileDecoderError::InvalidString => {
                write!(f, "InvalidString Error")
            }
            RdbFileDecoderError::InvalidExpirationConversion(err) => {
                write!(f, "InvalidExpirationConversion Error = {}", err)
            }
            RdbFileDecoderError::MissingDbIndex => {
                write!(f, "MissingDbIndex Error")
            }
            RdbFileDecoderError::EmptyBuffer => {
                write!(f, "EmptyBuffer Error")
            }
            RdbFileDecoderError::InvalidArrayConversion(err) => {
                write!(f, "InvalidArrayConversion Error = {}", err)
            }
        }
    }
}

// When buffer contains information, then read methods will first get information from the buffer before reading
// the file.
//
// If the buffer is smaller than the amount of bytes that we need to read, the result will be a combination between
// the buffer and the bytes read from the file.
//
// We keep the buffer in order to save system calls.
struct RdbFileReader {
    file: File,
    buffer: BytesMut,
}

impl RdbFileReader {
    fn new(file: File) -> Self {
        Self {
            file,
            buffer: BytesMut::new(),
        }
    }

    async fn read_exact(&mut self, number_of_bytes: usize) -> Result<Vec<u8>, RdbFileDecoderError> {
        let mut buf = BytesMut::with_capacity(number_of_bytes - self.buffer.len());

        buf.put(self.buffer.clone());

        self.file
            .read_buf(&mut buf)
            .await
            .map_err(|err| RdbFileDecoderError::ReadFile(err.to_string()))?;

        self.buffer.clear();

        Ok(buf.to_vec())
    }

    async fn read_u8(&mut self) -> Result<u8, RdbFileDecoderError> {
        if self.buffer.is_empty() {
            let byte = self
                .file
                .read_u8()
                .await
                .map_err(|err| RdbFileDecoderError::ReadFile(err.to_string()))?;

            return Ok(byte);
        }
        let cloned_buffer = self.buffer.clone();
        let last_byte = cloned_buffer.last().unwrap();

        self.buffer.clear();

        Ok(last_byte.to_owned())
    }

    // It writes a byte to the buffer
    //
    // This is useful when needing to read a byte in order to check if it's a section. In case it's not a section, we want to keep
    // that byte as it will be necessary for the next read.
    fn write_byte_to_buf(&mut self, byte: u8) {
        self.buffer.put_u8(byte);
    }
}

pub struct RdbFileDecoder {
    reader: RdbFileReader,
    current_section: RdbSection,
}

impl RdbFileDecoder {
    pub fn new(file: File) -> Self {
        Self {
            reader: RdbFileReader::new(file),
            // We assume that the first section of the .rdb file is going to be the header
            current_section: RdbSection::HEADER,
        }
    }

    pub async fn decode(&mut self) -> Result<RdbData, RdbFileDecoderError> {
        let mut builder = RdbDataBuilder::new();

        loop {
            match self.current_section {
                RdbSection::HEADER => {
                    let header_decoder = HeaderDecoder::new(self);
                    let header = header_decoder.decode().await?;

                    builder.with_header(header);
                    self.current_section = RdbSection::METADATA;
                }
                RdbSection::METADATA => {
                    let metadata_decoder = MetadataDecoder::new(self);
                    let metadata = metadata_decoder.decode().await?;

                    builder.with_metadata(metadata);
                }
                RdbSection::DATABASE => {
                    let databases_decoder = DatabasesDecoder::new(self);
                    let databases = databases_decoder.decode().await?;

                    builder.with_databases(databases);
                }
                _ => break,
            };
        }

        let data = builder.build();

        Ok(data)
    }
}

struct HeaderDecoder<'a> {
    rdb_decoder: &'a mut RdbFileDecoder,
}

impl<'a> HeaderDecoder<'a> {
    pub fn new(rdb_decoder: &'a mut RdbFileDecoder) -> HeaderDecoder<'a> {
        rdb_decoder.current_section = RdbSection::HEADER;

        HeaderDecoder { rdb_decoder }
    }

    pub async fn decode(mut self) -> Result<RdbDataHeader, RdbFileDecoderError> {
        let magic_string = self.decode_magic_string().await?;
        let version = self.decode_version().await?;

        Ok(RdbDataHeader {
            magic_string,
            version,
        })
    }

    async fn decode_magic_string(&mut self) -> Result<String, RdbFileDecoderError> {
        let magic_string_decoded = self.rdb_decoder.reader.read_exact(5).await?;

        String::from_utf8(magic_string_decoded)
            .map_err(RdbFileDecoderError::InvalidStringConversion)
    }

    async fn decode_version(&mut self) -> Result<String, RdbFileDecoderError> {
        let version_decoded = self.rdb_decoder.reader.read_exact(4).await?;

        String::from_utf8(version_decoded).map_err(RdbFileDecoderError::InvalidStringConversion)
    }
}

struct MetadataDecoder<'a> {
    rdb_decoder: &'a mut RdbFileDecoder,
}

impl<'a> MetadataDecoder<'a> {
    pub fn new(rdb_decoder: &'a mut RdbFileDecoder) -> MetadataDecoder<'a> {
        rdb_decoder.current_section = RdbSection::METADATA;

        MetadataDecoder { rdb_decoder }
    }

    pub async fn decode(self) -> Result<RdbDataMetadata, RdbFileDecoderError> {
        let mut metadata = RdbDataMetadata::new();

        loop {
            let byte = self.rdb_decoder.reader.read_u8().await?;

            // It checks if the next byte is a section. When it's a section different than metadata, it means that we already
            // read all metadata information
            //
            // We also set the rdb file decoder section.
            if let Some(section) = RdbSection::from_byte(byte) {
                if !matches!(section, RdbSection::METADATA) {
                    self.rdb_decoder.current_section = section;

                    break;
                } else {
                    continue;
                }
            }

            // We write to buffer because while checking the section we lost one byte that it's important for following operations
            self.rdb_decoder.reader.write_byte_to_buf(byte);

            let key = StringDecoder::new(self.rdb_decoder).decode().await?;

            let value = StringDecoder::new(self.rdb_decoder).decode().await?;

            if metadata.set_key(&key, &value).is_err() {
                continue;
            }
        }

        Ok(metadata)
    }
}

struct DatabasesDecoder<'a> {
    rdb_decoder: &'a mut RdbFileDecoder,
}

impl<'a> DatabasesDecoder<'a> {
    pub fn new(rdb_decoder: &'a mut RdbFileDecoder) -> DatabasesDecoder<'a> {
        rdb_decoder.current_section = RdbSection::DATABASE;

        DatabasesDecoder { rdb_decoder }
    }

    pub async fn decode(mut self) -> Result<RdbDataDatabases, RdbFileDecoderError> {
        let mut rdb_databases = RdbDataDatabases::default();

        loop {
            let byte = self.rdb_decoder.reader.read_u8().await?;

            // It checks if the next byte is a section. When it's a section, it means that we already
            // read all database information
            //
            // We also set the rdb file decoder section.
            if let Some(section) = RdbSection::from_byte(byte) {
                if !matches!(section, RdbSection::DATABASE) {
                    self.rdb_decoder.current_section = section;

                    break;
                } else {
                    continue;
                }
            }

            // We write to buffer because while checking the section we lost one byte that it's important for following operations
            self.rdb_decoder.reader.write_byte_to_buf(byte);

            let database = self.decode_database().await?;

            rdb_databases.add_database(database);
        }

        Ok(rdb_databases)
    }

    async fn decode_database(&mut self) -> Result<RdbDatabase, RdbFileDecoderError> {
        let db_index = self.decode_index().await?;

        match db_index {
            Size::Length(index) => {
                let (number_of_keys, number_of_expired_keys) =
                    self.decode_size_information().await?;

                let mut database = RdbDatabase::new(index, number_of_keys, number_of_expired_keys);

                // Get database values
                loop {
                    let byte = self.rdb_decoder.reader.read_u8().await?;

                    // We write to buffer because while checking the section we lost one byte that it's important for following operations
                    self.rdb_decoder.reader.write_byte_to_buf(byte);

                    // We stop to check for more values when there is a new section
                    if RdbSection::from_byte(byte).is_some() {
                        return Ok(database);
                    }

                    let (key, value) = self.decode_db_store_value().await?;

                    database.set(&key, value);
                }
            }
            _ => Err(RdbFileDecoderError::MissingDbIndex),
        }
    }

    async fn decode_index(&mut self) -> Result<Size, RdbFileDecoderError> {
        let db_index = SizeDecoder::new(self.rdb_decoder).decode().await?;

        Ok(db_index)
    }

    async fn decode_size_information(&mut self) -> Result<(Size, Size), RdbFileDecoderError> {
        let byte = self.rdb_decoder.reader.read_u8().await?;

        // Indicates that hash table size information follows.
        if format!("{:X}", byte).as_str() != "FB" {
            return Err(RdbFileDecoderError::InvalidSection(String::from(
                "Invalid database section: Size information is missing",
            )));
        }

        // Get size information
        let number_of_keys = SizeDecoder::new(self.rdb_decoder).decode().await?;
        let number_of_expired_keys = SizeDecoder::new(self.rdb_decoder).decode().await?;

        Ok((number_of_keys, number_of_expired_keys))
    }

    async fn decode_expire_timestamp_in_seconds(&mut self) -> Result<u32, RdbFileDecoderError> {
        let buf = self.rdb_decoder.reader.read_exact(4).await?;
        let buf: [u8; 4] = buf.try_into().map_err(|_| {
            RdbFileDecoderError::InvalidArrayConversion(String::from(
                "Array expected to have a length of 4",
            ))
        })?;

        let timestamp_in_seconds = u32::from_le_bytes(buf);

        Ok(timestamp_in_seconds)
    }

    async fn decode_expire_timestamp_in_miliseconds(&mut self) -> Result<u64, RdbFileDecoderError> {
        let buf = self.rdb_decoder.reader.read_exact(8).await?;
        let buf: [u8; 8] = buf.try_into().map_err(|_| {
            RdbFileDecoderError::InvalidArrayConversion(String::from(
                "Array expected to have a length of 8",
            ))
        })?;

        let timestamp_in_seconds = u64::from_le_bytes(buf);

        Ok(timestamp_in_seconds)
    }

    async fn decode_db_key_value_pairs(
        &mut self,
    ) -> Result<(String, Option<String>), RdbFileDecoderError> {
        let byte = self.rdb_decoder.reader.read_u8().await?;
        let value_type = RdbValueType::from_byte(byte);

        let key = StringDecoder::new(self.rdb_decoder).decode().await?;

        let value = match value_type {
            Some(RdbValueType::StringEncoding) => {
                let str_value = StringDecoder::new(self.rdb_decoder).decode().await?;

                Some(str_value)
            }
            _ => None,
        };

        Ok((key, value))
    }

    async fn decode_db_store_value(&mut self) -> Result<(String, StoreValue), RdbFileDecoderError> {
        let byte = self.rdb_decoder.reader.read_u8().await?;
        let mut store_value_builder = StoreValueBuilder::new();
        let mut key = String::new();

        match format!("{:X}", byte).as_str() {
            // Expire timestamp information in miliseconds
            "FC" => {
                let expire_timestamp_in_milis =
                    self.decode_expire_timestamp_in_miliseconds().await?;
                let expire_timestamp_in_milis = i64::try_from(expire_timestamp_in_milis)
                    .map_err(RdbFileDecoderError::InvalidExpirationConversion)?;

                let exp = DateTime::from_timestamp_millis(expire_timestamp_in_milis);

                if let Some(exp) = exp {
                    store_value_builder.with_exp(exp);
                }
            }
            // Expire timestamp information in seconds
            "FD" => {
                let expire_timestamp_in_seconds = self.decode_expire_timestamp_in_seconds().await?;
                let expire_timestamp_in_seconds = i32::try_from(expire_timestamp_in_seconds)
                    .map_err(RdbFileDecoderError::InvalidExpirationConversion)?;

                let exp = DateTime::from_timestamp(expire_timestamp_in_seconds.into(), 0);

                if let Some(exp) = exp {
                    store_value_builder.with_exp(exp);
                }
            }
            _ => {
                self.rdb_decoder.reader.write_byte_to_buf(byte);

                let db_value = self.decode_db_key_value_pairs().await?;

                if let Some(value) = db_value.1 {
                    store_value_builder.with_value(&value);
                }

                key = db_value.0;
            }
        }

        let store_value = store_value_builder.build();

        Ok((key, store_value))
    }
}

#[derive(Debug)]
enum Size {
    Length(usize),
    StringType(Bytes),
}

/// Length encoding is used to store the length of the next object in the stream.
/// Length encoding is a variable byte encoding designed to use as few bytes as possible.
struct SizeDecoder<'a> {
    rdb_decoder: &'a mut RdbFileDecoder,
}

impl<'a> SizeDecoder<'a> {
    fn new(rdb_decoder: &'a mut RdbFileDecoder) -> Self {
        Self { rdb_decoder }
    }

    async fn decode(&mut self) -> Result<Size, RdbFileDecoderError> {
        let byte = self.rdb_decoder.reader.read_u8().await?;

        match byte >> 6 {
            // If the first two bits are 0b00:
            // The size is the remaining 6 bits of the byte.
            0b00 => {
                let size = u8::from_be_bytes([byte.to_owned(); 1]);

                Ok(Size::Length(usize::from(size)))
            }
            // If the first two bits are 0b01:
            // The size is the next 14 bits(remaining 6 bits in the first byte, combined with the next byte), in big-endian (read left-to-right).
            0b01 => {
                let mut bytes = BytesMut::new();

                let first_byte = byte & 0b00111111;
                bytes.put_u8(first_byte);

                let next_byte = self.rdb_decoder.reader.read_u8().await?;
                bytes.put_u8(next_byte);

                let size = u16::from_be_bytes([bytes[0], bytes[1]]);

                Ok(Size::Length(usize::from(size)))
            }
            // If the first two bits are 0b10:
            // Ignore the remaining 6 bits of the first byte. The size is the next 4 bytes, in big-endian (read left-to-right).
            0b10 => {
                let buf = self.rdb_decoder.reader.read_exact(4).await?;
                let buf: [u8; 4] = buf.try_into().map_err(|_| {
                    RdbFileDecoderError::InvalidArrayConversion(String::from(
                        "Array expected to have a length of 4",
                    ))
                })?;

                let size = u32::from_be_bytes(buf);
                let size: usize = size.try_into().map_err(|_| {
                    RdbFileDecoderError::InvalidNumberConversion(String::from(
                        "Could not convert the value properly",
                    ))
                })?;

                Ok(Size::Length(size))
            }
            // If the first two bits are 0b11:
            // The remaining 6 bits specify a type of string encoding.
            0b11 => {
                let bytes = Bytes::from([byte.to_owned()].to_vec());

                Ok(Size::StringType(bytes))
            }
            _ => Err(RdbFileDecoderError::InvalidSize),
        }
    }
}

struct StringDecoder<'a> {
    rdb_decoder: &'a mut RdbFileDecoder,
}

impl<'a> StringDecoder<'a> {
    fn new(rdb_decoder: &'a mut RdbFileDecoder) -> Self {
        Self { rdb_decoder }
    }

    async fn decode(&mut self) -> Result<String, RdbFileDecoderError> {
        let mut size_decoder = SizeDecoder::new(self.rdb_decoder);
        let size = size_decoder.decode().await?;

        match size {
            Size::Length(length) => {
                let buf = self.rdb_decoder.reader.read_exact(length).await?;

                let value =
                    String::from_utf8(buf).map_err(RdbFileDecoderError::InvalidStringConversion)?;

                Ok(value)
            }
            Size::StringType(byte) => match format!("{:X}", byte).as_str() {
                // The 0xC0 size indicates the string is an 8-bit integer.
                "C0" => {
                    let byte = self.rdb_decoder.reader.read_u8().await?;

                    Ok(byte.to_string())
                }
                // The 0xC1 size indicates the string is a 16-bit integer.
                "C1" => {
                    let buf = self.rdb_decoder.reader.read_exact(2).await?;

                    let number = u16::from_le_bytes([buf[0], buf[1]]);

                    Ok(number.to_string())
                }
                // The 0xC2 size indicates the string is a 32-bit integer.
                "C2" => {
                    let buf = self.rdb_decoder.reader.read_exact(4).await?;
                    let buf: [u8; 4] = buf.try_into().map_err(|_| {
                        RdbFileDecoderError::InvalidArrayConversion(String::from(
                            "Array expected to have a length of 4",
                        ))
                    })?;

                    let number = u32::from_le_bytes(buf);

                    Ok(number.to_string())
                }
                //  The 0xC3 size indicates that the string is compressed with the LZF algorithm.
                "C3" => {
                    todo!("It is not necessary to implement a LZF algorithm for this challenge");
                }
                _ => Err(RdbFileDecoderError::InvalidString),
            },
        }
    }
}
