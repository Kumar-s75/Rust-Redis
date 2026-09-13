use rusty_cache::{handle_client, Database};
use std::env;
use std::io;
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

fn main() -> io::Result<()> {
    let address = env::var("RUSTY_CACHE_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".into());
    let data_file = env::var("RUSTY_CACHE_FILE").unwrap_or_else(|_| "rusty-cache.aof".into());
    let database = Database::open(&data_file)?;

    let cleaner_database = database.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(1));
        cleaner_database.remove_expired();
    });

    let listener = TcpListener::bind(&address)?;
    println!("rusty-cache listening on {address}; persistence: {data_file}");
    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                let database = database.clone();
                thread::spawn(move || {
                    if let Err(error) = handle_client(stream, database) {
                        eprintln!("client error: {error}");
                    }
                });
            }
            Err(error) => eprintln!("connection failed: {error}"),
        }
    }
    Ok(())
}
