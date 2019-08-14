//! The Disk Log file API

use crate::kvstorage::{Key, Value, KEY_SIZE, VALUE_SIZE};
use std::sync::Arc;
use std::error::Error;
use std::fs;
use std::io::{Read, Write};
use std::fmt;
use std::fmt::{Display, Formatter};

// Disk log format
//  -- 1 byte functionality
//     'P': put
//      -- KEY_SIZE bytes key
//      -- VALUE_SIZE bytes value
//     'D': delete
//      -- KEY_SIZE bytes key

const DISK_PUT: u8 = b'P';
const DISK_DELETE: u8 = b'D';

/// The error type used by disklog module
#[derive(Debug)]
pub struct DiskLogError {
    description: String
}

impl DiskLogError {
    pub fn new(description: &str) -> Self {
        DiskLogError { description: description.to_owned() }
    }
}

impl Display for DiskLogError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "disk log error: {}", self.description)
    }
}

impl Error for DiskLogError {
}

/// A disk log message read out from a file, or going to be write into a file
pub enum DiskLogMessage {
    Put(Key, Arc<Value>),
    Delete(Key)
}

impl DiskLogMessage {
    /// Serialize a `DiskLogMessage` into a byte buffer
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            DiskLogMessage::Put(key, value) => {
                let mut ret = vec![DISK_PUT];
                ret.append(&mut key.serialize());
                ret.append(&mut value.serialize());
                ret
            },
            DiskLogMessage::Delete(key) => {
                let mut ret = vec![DISK_DELETE];
                ret.append(&mut key.serialize());
                ret
            }
        }
    }
}

/// Reader for `DiskLogMessage`
pub struct DiskLogReader {
    disk_log_file: fs::File
}

/// Writer for `DiskLogMessage`
pub struct DiskLogWriter {
    disk_log_file: fs::File
}

impl DiskLogReader {
    /// Create a `DiskLogReader` with given `File`
    ///
    /// This function requires the given `File` to be opened with `read`, and the file pointer must
    /// be at the beginning of the file. If not, further operations may return Error
    pub fn new(disk_log_file: fs::File) -> Self {
        DiskLogReader { disk_log_file }
    }

    /// Try reading a log out of the file
    ///
    /// returns `None` if there is no more data (reaches EOF), `Err` if there's an error with file
    /// or disk log format
    pub fn next_log(&mut self) -> Result<Option<DiskLogMessage>, Box<dyn Error>> {
        let mut operate: [u8; 1] = [0];
        match self.disk_log_file.read_exact(&mut operate) {
            Ok(_) => {
                let mut key = [0u8; KEY_SIZE];
                self.disk_log_file.read_exact(&mut key)?;
                let key = Key::from_slice(&key);
                if operate[0] == DISK_PUT {
                    let mut value = [0u8; VALUE_SIZE];
                    self.disk_log_file.read_exact(&mut value)?;
                    let value = Value::from_slice(&value);
                    Ok(Some(DiskLogMessage::Put(key, Arc::new(value))))
                } else if operate[0] == DISK_DELETE {
                    Ok(Some(DiskLogMessage::Delete(key)))
                } else {
                    Err(Box::new(DiskLogError::new("incorrect disk log format")))
                }
            },
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof  {
                    Ok(None)
                } else {
                    Err(Box::new(e))
                }
            },
        }
    }
}

impl DiskLogWriter {
    /// Create a `DiskLogReader` with given `File`
    ///
    /// This function requires the given `File` to be opened with `write` + `append`, and the file
    /// pointer must be at the end of the file. If not, further operations may return Error
    pub fn new(disk_log_file: fs::File) -> Self {
        DiskLogWriter { disk_log_file }
    }

    /// Try write a log into the file
    ///
    /// returns `Err` if there's an error with file
    pub fn write(&mut self, msg: DiskLogMessage) -> Result<(), Box<dyn Error>> {
        self.disk_log_file.write(&msg.serialize())?;
        Ok(())
    }
}
