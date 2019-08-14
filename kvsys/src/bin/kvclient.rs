use std::net::TcpStream;
use std::{io, fmt};
use std::io::Write;
use std::error::Error;

use kvsys::kvstorage::{Key, Value};
use kvsys::kvclient::KVClient;

#[derive(Debug)]
pub struct ClientError {
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
    pub fn new(description: &str) -> Self {
        ClientError { description: description.to_owned() }
    }
}


fn main() {
    env_logger::init();

    println!("KV storage client -- v0.1");
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
    let mut client = KVClient::new(tcp_stream);
    loop {
        print!("kv-client> ");
        io::stdout().flush().unwrap();

        let mut command = String::new();
        io::stdin().read_line(&mut command).unwrap();
        match parse_command(command) {
            Ok(command) => exec_command(&mut client, command)?,
            Err(e) => println!("{}", e)
        }
    }
}

enum Command {
    Get(Key),
    Put(Key, Value),
    Scan(Key, Key),
    Delete(Key),
    Close
}

fn parse_command(command: String) -> Result<Command, ClientError> {
    let parts = command.trim().split_whitespace().collect::<Vec<_>>();
    if parts.len() == 0 {
        return Err(ClientError::new("no command given!"));
    }
    match parts[0] {
        "get" => {
            if parts.len() != 2 {
                return Err(ClientError::new("`get` requires exactly 1 argument"))
            }
            let key = check_key_size(&parts[1].as_bytes())?;
            Ok(Command::Get(key))
        },
        "put" => {
            if parts.len() != 3 {
                return Err(ClientError::new("put requires exactly 2 arguments"))
            }

            let key = check_key_size(parts[1].as_bytes())?;
            let value = check_value_size(parts[2].as_bytes())?;
            Ok(Command::Put(key, value))
        },
        "scan" => {
            if parts.len() != 3 {
                return Err(ClientError::new("scan requires exactly 2 arguments"))
            }

            let key1 = check_key_size(parts[1].as_bytes())?;
            let key2 = check_key_size(parts[2].as_bytes())?;
            Ok(Command::Scan(key1, key2))
        },
        "del" | "delete" => {
            if parts.len() != 2 {
                return Err(ClientError::new("delete requires exactly 1 argument"))
            }
            let key = check_key_size(parts[1].as_bytes())?;
            Ok(Command::Delete(key))
        },
        "close" => {
            Ok(Command::Close)
        }
        _ => {
            Err(ClientError::new("unknown command"))
        }
    }
}

fn exec_command(client: &mut KVClient, command: Command) -> Result<(), Box<dyn Error>> {
    match command {
        Command::Get(key) => {
            client.do_get(key, handle_get_result)
        },
        Command::Put(key, value) => {
            client.do_put(key, value)?;
            println!("  Done");
            Ok(())
        },
        Command::Scan(key1, key2) => {
            client.do_scan(key1, key2, handle_scan_result)?;
            Ok(())
        },
        Command::Delete(key) => {
            client.do_delete(key, handle_delete_result)
        },
        Command::Close => {
            Ok(client.do_close())
        }
    }
}

fn handle_get_result(value: Option<Value>) -> () {
    if let Some(value) = value {
        println!("  {}", value);
    } else {
        println!("  Nil");
    }
}

fn handle_delete_result(rows_affected: usize) -> () {
    println!("  Ok, {} rows affected", rows_affected)
}

fn handle_scan_result(kv_pairs: Vec<(Key, Value)>) -> () {
    for (key, value) in kv_pairs.iter() {
        println!("  {} => {}", key, value)
    }
}

fn check_key_size(slice: &[u8]) -> Result<Key, ClientError> {
    Key::from_slice_checked(slice).ok_or(ClientError::new("incorrect key size"))
}

fn check_value_size(slice: &[u8]) -> Result<Value, ClientError> {
    if slice.len() < 256 {
        let mut ret = [0; 256];
        for i in 0..slice.len() {
            ret[i] = slice[i];
        }
        Ok(Value::from_slice(&ret))
    } else {
        Value::from_slice_checked(slice).ok_or(ClientError::new("incorrect value size"))
    }
}
