pub mod config;
pub use config::KVServerConfig;

use std::{fs, path, process};
use std::net::{TcpListener, SocketAddr};
use std::sync::{Arc, RwLock};

use crate::kvstorage::KVStorage;

use threads_pool::*;
use log::{error, warn, info};
use std::error::Error;
use std::io::Write;

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
    let tcp_listener = TcpListener::bind(&addr);
    match tcp_listener {
        Ok(tcp_listener) => tcp_listener,
        Err(e) => {
            error!("failed binding to port {}", config.listen_port);
            error!("extra info: {}", e.description());
            process::exit(1)
        }
    }
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
        let mut stream = stream.unwrap();

        let storage = storage.clone();
        let _ = pool.execute(move || {
            storage.write().unwrap().put(&[0; 8], [0; 256]);
            let _ = stream.flush();
        });
    }
}
