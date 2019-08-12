use std::net::TcpStream;
use std::io;
use std::io::Write;
use std::error::Error;
use std::fmt;

use kvsys::chunktps::ChunktpsConnection;
use kvsys::kvstorage::{KEY_SIZE, VALUE_SIZE, key_from_slice, value_from_slice, Key, Value};
use kvsys::kvserver::protocol::{Request, ReplyChunk};

#[derive(Debug)]
struct ClientError {
    description: String
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "client error: {}", self.description)
    }
}

impl Error for ClientError {

}

impl ClientError {
    fn new(description: &str) -> Self {
        ClientError { description: description.to_owned() }
    }
}

fn main() {
    env_logger::init();

    print!("KV storage client -- v0.1");
    print!("server IP:PORT to connect: ");
    io::stdout().flush().unwrap();

    let mut ip_addr = String::new();
    io::stdin().read_line(&mut ip_addr).unwrap();
    match TcpStream::connect(ip_addr.trim()) {
        Ok(tcp_stream) => {
            if let Err(e) = mainloop(tcp_stream) {
                eprintln!("critical error occurred in client mainloop, client shutting down");
                eprintln!("detailed error info: {}", e);
            }
        }
        Err(e) => {
            eprintln!("critical error occurred while opening TCP connection, client shutting down");
            eprintln!("detailed error info: {}", e);
        }
    }
}

fn mainloop(tcp_stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut chunktps = ChunktpsConnection::new(tcp_stream);
    loop {
        print!("kv-client> ");
        io::stdout().flush().unwrap();

        let mut command = String::new();
        io::stdin().read_line(&mut command).unwrap();
        match parse_input(command) {
            Ok(request) => {
                chunktps.write_chunk(request.serialize())?;

            },
            Err(e) => {
                eprintln!("{}", e.description())
            }
        }
    }
}

fn parse_input(command: String) -> Result<Request, ClientError> {
    let parts = command.trim().split_whitespace().collect::<Vec<_>>();
    if parts.len() == 0 {
        return Err(ClientError::new("no command given!"))
    }
    match parts[0] {
        "get" => {
            if parts.len() != 2 {
                return Err(ClientError::new("`get` requires exactly 1 argument"))
            }
            let key = check_key_size(parts[1].as_bytes())?;
            Ok(Request::Get(key))
        },
        "put" => {
            if parts.len() != 3 {
                return Err(ClientError::new("put requires exactly 2 arguments"))
            }

            let key = check_key_size(parts[1].as_bytes())?;
            let value = check_value_size(parts[2].as_bytes())?;
            Ok(Request::Put(key, value))
        },
        "scan" => {
            if parts.len() != 3 {
                return Err(ClientError::new("scan requires exactly 2 arguments"))
            }

            let key1 = check_key_size(parts[1].as_bytes())?;
            let key2 = check_key_size(parts[2].as_bytes())?;
            Ok(Request::Scan(key1, key2))
        },
        "del" | "delete" => {
            if parts.len() != 2 {
                return Err(ClientError::new("delete requires exactly 1 argument"))
            }

            let key = check_key_size(parts[1].as_bytes())?;
            Ok(Request::Del(key))
        },
        "close" => {
            if parts.len() != 1 {
                eprintln!("close requires no argument");
            }
            Ok(Request::Close)
        }
        _ => {
            Err(ClientError::new("unknown command"))
        }
    }
}

fn check_key_size(key_bytes: &[u8]) -> Result<Key, ClientError> {
    if key_bytes.len() != KEY_SIZE {
        Err(ClientError::new("incorrect key size"))
    } else {
        Ok(key_from_slice(key_bytes))
    }
}

fn check_value_size(value_bytes: &[u8]) -> Result<Value, ClientError> {
    if value_bytes.len() != VALUE_SIZE {
        Err(ClientError::new("incorrect value size"))
    } else {
        Ok(value_from_slice(value_bytes))
    }
}