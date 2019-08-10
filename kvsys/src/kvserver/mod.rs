pub mod config;
pub use config::KVServerConfig;

use std::{fs, path, process};
use std::net::{TcpListener, SocketAddr};
use std::sync::{Arc, RwLock, Mutex};

use crate::kvstorage::KVStorage;

use threads_pool::*;
use log::{error, warn, info};
use std::error::Error;
use std::io::Write;

pub fn run_server(config: KVServerConfig) {
    let path = path::Path::new(&config.db_file);
    let is_existing = path.exists();
    let file = if is_existing {
        fs::File::open(path)
    } else {
        fs::File::create(path)
    };

    if let Err(e) = file {
        error!("failed opening or creating file {}", config.db_file);
        process::exit(1);
    }
    let file = file.unwrap();

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
    let storage = Arc::new(Mutex::new(storage));

    let addr = SocketAddr::from(([127, 0, 0, 1], config.listen_port));
    let tcp_listener = TcpListener::bind(&addr);
    if let Err(e) = tcp_listener {
        error!("failed binding to port {}", config.listen_port);
        process::exit(1);
    }
    let tcp_listener = tcp_listener.unwrap();
    let pool = ThreadPool::new(config.threads as usize);

    for stream in tcp_listener.incoming() {
        if let Err(e) = stream {
            warn!("an TCP error occurred, extra info: {}", e.description());
            info!("automatically gave up and moved to next iteration");
            break;
        }
        let mut stream = stream.unwrap();

        let storage = storage.clone();
        pool.execute(move || {
            storage.lock().unwrap().put(&[0; 8], [0; 256]);
            stream.flush();
        });
    }
}
