use std::collections::BTreeMap;
use std::{thread, fs};
use std::sync::mpsc;
use std::io::{Read, Write};
use std::ops::Bound::{Included, Excluded};
use std::error::Error;
use std::thread::JoinHandle;

pub type Key = [u8; 8];
pub type Value = [u8; 256];
type InternKey = u64;

enum DiskLogMessage { Put(Key, Value), Delete(Key), Shutdown }

#[allow(dead_code)]
pub struct KVStorage {
    mem_storage: BTreeMap<InternKey, Option<Value>>,
    disk_log_thread: thread::JoinHandle<()>,
    disk_log_sender: mpsc::Sender<DiskLogMessage>
}

impl KVStorage {
    pub fn new(log_file: fs::File) -> Self {
        let (sender, log_thread) = KVStorage::create_disk_logger(log_file);
        KVStorage{ mem_storage: BTreeMap::new(), disk_log_thread: log_thread, disk_log_sender: sender }
    }

    pub fn from_existing_file(mut log_file: fs::File) -> Result<Self, Box<dyn Error>> {
        let mut mem_storage = BTreeMap::new();

        let mut operate: [u8; 1] = [0];
        while log_file.read_exact(&mut operate).is_ok() {
            let mut key: [u8; 8] = [0; 8];
            log_file.read_exact(&mut key)?;
            if operate[0] == b'P' {
                let mut value: [u8; 256] = [0; 256];
                log_file.read_exact(&mut value)?;
                mem_storage.insert(KVStorage::encode_key(&key), Some(value));
            }
            else if operate[0] == b'D' {
                mem_storage.remove(&KVStorage::encode_key(&key));
            }
        }

        let (sender, log_thread) = KVStorage::create_disk_logger(log_file);
        Ok(KVStorage{ mem_storage, disk_log_sender: sender, disk_log_thread: log_thread })
    }

    pub fn get(&self, key: &Key) -> &Option<Value> {
        let encoded_key = KVStorage::encode_key(key);
        if let Some(maybe_value) = self.mem_storage.get(&encoded_key) {
            maybe_value
        }
        else {
            &None
        }
    }

    pub fn put(&mut self, key: &Key, value: Value) {
        let encoded_key = KVStorage::encode_key(key);
        self.disk_log_sender.send(DiskLogMessage::Put(*key, value)).unwrap();
        self.mem_storage.insert(encoded_key, Some(value));
    }

    pub fn delete(&mut self, key: &Key) -> usize {
        let encoded_key = KVStorage::encode_key(key);
        if let Some(maybe_value) = self.mem_storage.get_mut(&encoded_key) {
            self.disk_log_sender.send(DiskLogMessage::Delete(*key)).unwrap();
            *maybe_value = None;
            1
        } else {
            0
        }
    }

    pub fn scan(&self, key1: &Key, key2: &Key) -> Vec<(Key, &Option<Value>)> {
        let (encoded_key1, encoded_key2) = (KVStorage::encode_key(key1), KVStorage::encode_key(key2));
        self.mem_storage.range((Included(encoded_key1), Excluded(encoded_key2)))
            .filter(|x| {
                let (_, v) = x;
                if let Some(_) = v { true } else { false }
            })
            .map(|x| {
                let (k, v) = x;
                (KVStorage::decode_key(*k), v)
            })
            .collect::<Vec<_>>()
    }

    pub fn shutdown(self) {
        self.disk_log_sender.send(DiskLogMessage::Shutdown).unwrap();
        self.disk_log_thread.join().unwrap();
    }

    fn encode_key(flat: &Key) -> InternKey {
        unsafe {
            let flat = flat as *const u8 as *const u64;
            (*flat).swap_bytes()
        }
    }

    fn decode_key(encoded: InternKey) -> Key {
        unsafe {
            let bytes = &(encoded.swap_bytes()) as *const u64 as *const [u8; 8];
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
    use crate::KVStorage;

    #[test]
    fn test_encode_key() {
        let flat: [u8; 8] = [0x40, 0x49, 0x0f, 0xd0, 0xca, 0xfe, 0xba, 0xbe];
        let expected = 0x40490fd0cafebabeu64;
        let encoded = KVStorage::encode_key(&flat);
        assert_eq!(encoded, expected);

        let decoded = KVStorage::decode_key(encoded);
        assert_eq!(decoded, flat);
    }

    #[test]
    fn test_encode_key_2() {
        let flat: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x3c, 0x9a, 0x0e];
        let expected = 0x3c9a0eu64;
        let encoded = KVStorage::encode_key(&flat);
        assert_eq!(encoded, expected);

        let decoded = KVStorage::decode_key(encoded);
        assert_eq!(decoded, flat);
    }
}
