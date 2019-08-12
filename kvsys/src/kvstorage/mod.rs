use std::collections::BTreeMap;
use std::{thread, fs};
use std::sync::mpsc;
use std::io::{Read, Write};
use std::ops::Bound::{Included, Excluded};
use std::error::Error;
use std::thread::JoinHandle;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use std::u64;

pub const KEY_SIZE: usize = 8;
pub const VALUE_SIZE: usize = 256;

pub type Key = [u8; KEY_SIZE];
pub type Value = [u8; VALUE_SIZE];
type InternKey = u64;

pub fn key_from_slice(slice: &[u8]) -> Key {
    assert_eq!(slice.len(), KEY_SIZE);
    let mut ret = [0; KEY_SIZE];
    ret.copy_from_slice(slice);
    ret
}

pub fn value_from_slice(slice: &[u8]) -> Value {
    assert_eq!(slice.len(), VALUE_SIZE);
    let mut ret = [0; VALUE_SIZE];
    ret.copy_from_slice(slice);
    ret
}

enum DiskLogMessage { Put(Key, Arc<Value>), Delete(Key), Shutdown }

#[allow(dead_code)]
pub struct KVStorage {
    mem_storage: BTreeMap<InternKey, Option<Arc<Value>>>,
    disk_log_sender: Mutex<mpsc::Sender<DiskLogMessage>>,
    disk_log_thread: Option<thread::JoinHandle<()>>
}

impl KVStorage {
    fn format_value(value: &Value) -> String {
        let mut ret = String::new();
        for &n in value.iter() {
            ret.push_str(format!("{:x}", n).as_str());
        }
        ret
    }
}

impl Debug for KVStorage {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "KV [")?;
        for (key, maybe_value) in self.mem_storage.iter() {
            if let Some(value) = maybe_value {
                write!(f, "{} => {},", key, KVStorage::format_value(value))?;
            }
        }
        write!(f, "]")
    }
}

impl Drop for KVStorage {
    fn drop(&mut self) {
        self.disk_log_sender.lock().unwrap().send(DiskLogMessage::Shutdown).unwrap();
        self.disk_log_thread.take().unwrap().join().unwrap();
    }
}

impl KVStorage {
    pub fn new(log_file: fs::File) -> Self {
        let (sender, log_thread) = KVStorage::create_disk_logger(log_file);
        KVStorage{ mem_storage: BTreeMap::new(), disk_log_sender: Mutex::new(sender), disk_log_thread: Some(log_thread) }
    }

    pub fn from_existing_file(mut log_file: fs::File) -> Result<Self, Box<dyn Error>> {
        let mut mem_storage = BTreeMap::new();

        let mut operate: [u8; 1] = [0];
        while log_file.read_exact(&mut operate).is_ok() {
            let mut key = [0u8; KEY_SIZE];
            log_file.read_exact(&mut key)?;
            if operate[0] == b'P' {
                let mut value = [0u8; VALUE_SIZE];
                log_file.read_exact(&mut value)?;
                mem_storage.insert(KVStorage::encode_key(&key), Some(Arc::new(value)));
            }
            else if operate[0] == b'D' {
                mem_storage.remove(&KVStorage::encode_key(&key));
            }
        }

        let (sender, log_thread) = KVStorage::create_disk_logger(log_file);
        Ok(KVStorage{ mem_storage, disk_log_sender: Mutex::new(sender), disk_log_thread: Some(log_thread) })
    }

    pub fn get(&self, key: &Key) -> Option<Arc<Value>> {
        let encoded_key = KVStorage::encode_key(key);
        if let Some(maybe_value) = self.mem_storage.get(&encoded_key) {
            (*maybe_value).clone()
        }
        else {
            None
        }
    }

    pub fn put(&mut self, key: &Key, value: &Value) {
        let encoded_key = KVStorage::encode_key(key);
        let value = Arc::new(*value);
        self.disk_log_sender.lock().unwrap().send(DiskLogMessage::Put(*key, value.clone())).unwrap();
        self.mem_storage.insert(encoded_key, Some(value));
    }

    pub fn delete(&mut self, key: &Key) -> usize {
        let encoded_key = KVStorage::encode_key(key);
        if let Some(maybe_value) = self.mem_storage.get_mut(&encoded_key) {
            self.disk_log_sender.lock().unwrap().send(DiskLogMessage::Delete(*key)).unwrap();
            *maybe_value = None;
            1
        } else {
            0
        }
    }

    pub fn scan(&self, key1: &Key, key2: &Key) -> Vec<(Key, Arc<Value>)> {
        let (encoded_key1, encoded_key2) = (KVStorage::encode_key(key1), KVStorage::encode_key(key2));
        self.mem_storage.range((Included(encoded_key1), Excluded(encoded_key2)))
            .filter(|x| {
                let (_, v) = x;
                if let Some(_) = v { true } else { false }
            })
            .map(|x| {
                let (k, v) = x;
                (KVStorage::decode_key(*k), v.as_ref().unwrap().clone())
            })
            .collect::<Vec<_>>()
    }

    fn encode_key(flat: &Key) -> InternKey {
        unsafe {
            let flat = flat as *const u8 as *const u64;
            u64::from_be(*flat)
        }
    }

    fn decode_key(encoded: InternKey) -> Key {
        unsafe {
            let bytes = &(u64::to_be(encoded)) as *const u64 as *const [u8; 8];
            *bytes
        }
    }

    fn serialize(message: &DiskLogMessage) -> Vec<u8> {
        match message {
            DiskLogMessage::Put(key, value) => {
                let mut ret = b"P".to_vec();
                ret.append(&mut key.to_vec());
                ret.append(&mut value.to_vec());
                ret
            },
            DiskLogMessage::Delete(key) => {
                let mut ret = b"D".to_vec();
                ret.append(&mut key.to_vec());
                ret
            },
            DiskLogMessage::Shutdown => {
                unreachable!()
            }
        }
    }

    fn create_disk_logger(mut log_file: fs::File) -> (mpsc::Sender<DiskLogMessage>, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel::<DiskLogMessage>();
        let log_thread = thread::spawn(move || {
            loop {
                let message = receiver.recv().unwrap();
                if let DiskLogMessage::Shutdown = message {
                    break;
                }
                log_file.write(&KVStorage::serialize(&message)).unwrap();
            }
        });
        (sender, log_thread)
    }
}

#[cfg(test)]
mod tests {
    use crate::kvstorage::KVStorage;

    #[test]
    fn test_encode_key() {
        let flat = [0x40u8, 0x49, 0x0f, 0xd0, 0xca, 0xfe, 0xba, 0xbe];
        let expected = 0x40490fd0cafebabeu64;
        let encoded = KVStorage::encode_key(&flat);
        assert_eq!(encoded, expected);

        let decoded = KVStorage::decode_key(encoded);
        assert_eq!(decoded, flat);
    }

    #[test]
    fn test_encode_key_2() {
        let flat = [0x00u8, 0x00, 0x00, 0x00, 0x00, 0x3c, 0x9a, 0x0e];
        let expected = 0x3c9a0eu64;
        let encoded = KVStorage::encode_key(&flat);
        assert_eq!(encoded, expected);

        let decoded = KVStorage::decode_key(encoded);
        assert_eq!(decoded, flat);
    }
}
