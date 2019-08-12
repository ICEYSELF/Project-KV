pub mod config;
pub mod protocol;
pub use config::KVServerConfig;
pub use protocol::{SCAN, PUT, GET, DEL};

use std::{fs, path, process};
use std::net::{TcpListener, SocketAddr, TcpStream};
use std::sync::{Arc, RwLock};

use crate::kvstorage::{KVStorage};
use crate::threadpool::ThreadPool;
use crate::kvserver::protocol::{Request, ServerReplyChunk, KV_PAIR_SERIALIZED_SIZE};
use crate::chunktps::{ChunktpsConnection, CHUNK_MAX_SIZE};

use log::{error, warn, info};
use std::error::Error;

fn create_storage_engine(config: &KVServerConfig) -> Arc<RwLock<KVStorage>> {
    let path = path::Path::new(&config.db_file);
    let is_existing = path.exists();
    let file = if is_existing {
        fs::File::open(path)
    } else {
        fs::File::create(path)
    }.unwrap_or_else(
        | e | {
            error!("failed opening or creating file {}", config.db_file);
            error!("extra info: {}", e.description());
            process::exit(1)
        }
    );

    let storage = if is_existing {
        KVStorage::from_existing_file(file).unwrap_or_else(| e | {
            error!("error setting up storage engine from existing file {}", config.db_file);
            error!("extra info: {}", e.description());
            error!("this is usually because you have a corrupted database file, or using a non-kv file");
            process::exit(1)
        })
    } else {
        KVStorage::new(file)
    };

    Arc::new(RwLock::new(storage))
}

fn bind_tcp_listener(config: &KVServerConfig) -> TcpListener {
    let addr = SocketAddr::from(([127, 0, 0, 1], config.listen_port));
    TcpListener::bind(&addr).unwrap_or_else(
        | e | {
            error!("failed binding to port {}", config.listen_port);
            error!("extra info: {}", e.description());
            process::exit(1)
        }
    )
}

pub fn run_server(config: KVServerConfig) {
    let storage = create_storage_engine(&config);
    let tcp_listener = bind_tcp_listener(&config);
    let pool = ThreadPool::new(config.threads as usize);

    for stream in tcp_listener.incoming() {
        if let Err(e) = stream {
            warn!("an TCP error occurred, extra info: {}", e.description());
            info!("automatically gave up and moved to next iteration");
            break;
        }
        let stream = stream.unwrap();

        let storage = storage.clone();
        pool.execute(move || {
            if let Err(e) = handle_connection(stream, storage) {
                warn!("an error occurred when processing request");
                info!("detailed error info: {}", e.description());
            }
        });
    }
}

fn handle_connection(stream: TcpStream, storage_engine: Arc<RwLock<KVStorage>>) -> Result<(), Box<dyn Error>> {
    let mut chunktps = ChunktpsConnection::new(stream);
    loop {
        match Request::deserialize_from(chunktps.read_chunk()?)? {
            Request::Get(key) => {
                let maybe_value = storage_engine.read().unwrap().get(&key);
                chunktps.write_chunk(ServerReplyChunk::SingleValue(maybe_value).serialize())?;
            },
            Request::Put(key, value) => {
                storage_engine.write().unwrap().put(&key, &value);
            },
            Request::Del(key) => {
                let rows_effected = storage_engine.write().unwrap().delete(&key);
                chunktps.write_chunk(ServerReplyChunk::Number(rows_effected).serialize())?;
            },
            Request::Scan(key1, key2) => {
                const ROW_PER_CHUNK: usize = (CHUNK_MAX_SIZE - 1) / KV_PAIR_SERIALIZED_SIZE;
                let scan_result = storage_engine.read().unwrap().scan(&key1, &key2);
                for i in (0..scan_result.len()).step_by(ROW_PER_CHUNK) {
                    let slice = if i + ROW_PER_CHUNK < scan_result.len() {
                        &scan_result[i..i+ROW_PER_CHUNK]
                    } else {
                        &scan_result[i..scan_result.len()]
                    };
                    chunktps.write_chunk(ServerReplyChunk::KVPairs(slice).serialize())?;
                }
                chunktps.write_chunk(vec![])?;
            },
            Request::Close => {
                return Ok(())
            }
        }
    }
}

#[cfg(test)]
mod test_server_handle_connection {
    use crate::kvstorage::KVStorage;
    use crate::util::{gen_key, gen_value, gen_key_n};
    use crate::chunktps::ChunktpsConnection;
    use crate::kvserver::handle_connection;
    use crate::kvserver::protocol::{Request, ReplyChunk};

    use std::sync::{Arc, RwLock};
    use std::net::{TcpStream, TcpListener};
    use std::{fs, thread};
    use std::time::Duration;
    use std::ops::Deref;

    #[test]
    fn test_handle_put() {
        let _ = fs::remove_file("test_put.kv");
        let log_file = fs::File::create("test_put.kv").unwrap();
        let storage_engine = Arc::new(RwLock::new(KVStorage::new(log_file)));
        let storage_engine_clone = storage_engine.clone();
        let t = thread::spawn(move || {
            let tcp_listener = TcpListener::bind("127.0.0.1:1972").unwrap();
            let (tcp_stream, _) = tcp_listener.accept().unwrap();
            handle_connection(tcp_stream, storage_engine_clone).unwrap();
        });

        let key = gen_key();
        let value = gen_value();

        thread::sleep(Duration::from_secs(1));
        let tcp_stream = TcpStream::connect("127.0.0.1:1972").unwrap();
        let mut chunktps = ChunktpsConnection::new(tcp_stream);
        chunktps.write_chunk(Request::Put(key, value).serialize()).unwrap();
        chunktps.write_chunk(Request::Close.serialize()).unwrap();

        t.join().unwrap();
        assert_eq!(storage_engine.read().unwrap().get(&key).unwrap().data.to_vec(), value.data.to_vec());
    }

    #[test]
    fn test_handle_get() {
        let _ = fs::remove_file("test_get.kv");
        let log_file = fs::File::create("test_get.kv").unwrap();
        let storage_engine = Arc::new(RwLock::new(KVStorage::new(log_file)));
        let key = gen_key();
        let value = gen_value();
        storage_engine.write().unwrap().put(&key, &value);
        let storage_engine_clone = storage_engine.clone();
        let t = thread::spawn(move || {
            let tcp_listener = TcpListener::bind("127.0.0.1:2333").unwrap();
            let (tcp_stream, _) = tcp_listener.accept().unwrap();
            handle_connection(tcp_stream, storage_engine_clone).unwrap();
        });

        thread::sleep(Duration::from_secs(1));
        let tcp_stream = TcpStream::connect("127.0.0.1:2333").unwrap();
        let mut chunktps = ChunktpsConnection::new(tcp_stream);
        chunktps.write_chunk(Request::Get(key).serialize()).unwrap();
        let reply = ReplyChunk::deserialize(chunktps.read_chunk().unwrap()).unwrap();
        match reply {
            ReplyChunk::SingleValue(v) => {
                assert_eq!(v.unwrap(), value)
            },
            _ => panic!()
        }

        chunktps.write_chunk(Request::Close.serialize()).unwrap();
        t.join().unwrap();
    }

    #[test]
    fn test_handle_scan() {
        let _ = fs::remove_file("test_scan.kv");
        let log_file = fs::File::create("test_scan.kv").unwrap();
        let storage_engine = Arc::new(RwLock::new(KVStorage::new(log_file)));
        for i in 0..255 {
            let key = gen_key_n(i);
            let value = gen_value();
            storage_engine.write().unwrap().put(&key, &value);
        }

        let storage_engine_clone = storage_engine.clone();
        let t = thread::spawn(move || {
            let tcp_listener = TcpListener::bind("127.0.0.1:4396").unwrap();
            let (tcp_stream, _) = tcp_listener.accept().unwrap();
            handle_connection(tcp_stream, storage_engine_clone).unwrap();
        });
        thread::sleep(Duration::from_secs(1));
        let tcp_stream = TcpStream::connect("127.0.0.1:4396").unwrap();
        let mut chunktps = ChunktpsConnection::new(tcp_stream);
        chunktps.write_chunk(Request::Scan(gen_key_n(0), gen_key_n(254)).serialize()).unwrap();

        let mut total_data = 0;
        loop {
            let data = chunktps.read_chunk().unwrap();
            if data.len() == 0 {
                break;
            }
            let chunk = ReplyChunk::deserialize(data).unwrap();
            match chunk {
                ReplyChunk::KVPairs(kv_pairs) => {
                    total_data += kv_pairs.len();
                    for (k, v) in kv_pairs.iter() {
                        let value = storage_engine.read().unwrap().get(k).unwrap();
                        assert_eq!(value.deref(), v);
                    }
                },
                _ => panic!()
            }
        }
        assert_eq!(total_data, 254);

        chunktps.write_chunk(Request::Close.serialize()).unwrap();
        t.join().unwrap();
    }
}
