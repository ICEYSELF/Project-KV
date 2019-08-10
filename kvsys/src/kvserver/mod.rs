pub mod config;
pub mod protocol;
pub use config::KVServerConfig;
pub use protocol::{SCAN, PUT, GET, DEL};

use std::{fs, path, process};
use std::net::{TcpListener, SocketAddr, TcpStream};
use std::sync::{Arc, RwLock};

use crate::kvstorage::KVStorage;
use crate::threadpool::ThreadPool;

use log::{error, warn, info};
use std::error::Error;
#[allow(unused_imports)]
use std::io::{Write, Read};

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

fn handle_connection(mut stream: TcpStream, storage_engine: Arc<RwLock<KVStorage>>) -> Result<(), Box<dyn Error>> {
    let mut command = [0u8];
    stream.read_exact(&mut command)?;
    match command[0] {
        SCAN => {
            let mut key0 = [0u8; 8];
            let mut key1 = [0u8; 8];
            stream.read_exact(&mut key0)?;
            stream.read_exact(&mut key1)?;
            info!("command used by client: SCAN {:?} {:?}", key0.to_vec(), key1.to_vec());
            let _ret = storage_engine.read().unwrap().scan(&key0, &key1);
            Ok(())
        },
        PUT => {
            let mut key = [0u8; 8];
            let mut value = [0u8; 256];
            stream.read_exact(&mut key)?;
            stream.read_exact(&mut value)?;
            info!("command used by client: PUT {:?} VALUE", key.to_vec());
            let _ret = storage_engine.write().unwrap().put(&key, &value);
            Ok(())
        },
        GET => {
            let mut key = [0u8; 8];
            stream.read_exact(&mut key)?;
            info!("command used by client: GET {:?}", key.to_vec());
            let _ret = storage_engine.read().unwrap().get(&key);
            Ok(())
        },
        DEL => {
            let mut key = [0u8; 8];
            stream.read_exact(&mut key)?;
            info!("command used by client: DEL {:?}", key.to_vec());
            let _ret = storage_engine.write().unwrap().delete(&key);
            Ok(())
        },
        _ => {
            warn!("invalid command: {}", command[0]);
            info!("maybe someone misused the client side API, or used incorrect client");
            info!("invalid command was ignored by default");
            Ok(())
        }
    }
}
