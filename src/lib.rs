use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    value: Vec<u8>,
    expires_at_ms: Option<u64>,
}

pub struct Database {
    entries: RwLock<HashMap<Vec<u8>, Entry>>,
    persistence: Mutex<File>,
}

pub type SharedDatabase = Arc<Database>;

impl Database {
    pub fn open(path: impl AsRef<Path>) -> io::Result<SharedDatabase> {
        let path = path.as_ref();
        let mut entries = HashMap::new();
        if path.exists() {
            let file = File::open(path)?;
            replay(BufReader::new(file), &mut entries)?;
        }
        entries.retain(|_, entry| !expired(entry));
        let persistence = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Arc::new(Self {
            entries: RwLock::new(entries),
            persistence: Mutex::new(persistence),
        }))
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        {
            let entries = self.entries.read().expect("database lock poisoned");
            if let Some(entry) = entries.get(key) {
                if !expired(entry) {
                    return Some(entry.value.clone());
                }
            } else {
                return None;
            }
        }
        self.entries.write().expect("database lock poisoned").remove(key);
        None
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>, expires_at_ms: Option<u64>) -> io::Result<()> {
        // Mutations are logged while holding the write lock, so replay order always
        // matches the order seen by clients.
        let mut entries = self.entries.write().expect("database lock poisoned");
        let mut log = self.persistence.lock().expect("persistence lock poisoned");
        append_set(&mut log, &key, &value, expires_at_ms)?;
        entries.insert(key, Entry { value, expires_at_ms });
        Ok(())
    }

    fn delete(&self, keys: &[Vec<u8>]) -> io::Result<usize> {
        let mut entries = self.entries.write().expect("database lock poisoned");
        let mut log = self.persistence.lock().expect("persistence lock poisoned");
        append_delete(&mut log, keys)?;
        let deleted = keys.iter().filter(|key| entries.remove(key.as_slice()).is_some()).count();
        Ok(deleted)
    }

    pub fn remove_expired(&self) -> usize {
        let mut entries = self.entries.write().expect("database lock poisoned");
        let before = entries.len();
        entries.retain(|_, entry| !expired(entry));
        before - entries.len()
    }
}

pub fn handle_client(stream: TcpStream, database: SharedDatabase) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    loop {
        let command = match read_command(&mut reader) {
            Ok(Some(command)) => command,
            Ok(None) => return Ok(()),
            Err(error) => {
                write_error(&mut writer, &format!("protocol error: {error}"))?;
                return Ok(());
            }
        };
        let quitting = command.first().is_some_and(|part| part.eq_ignore_ascii_case(b"QUIT"));
        execute(&database, command, &mut writer)?;
        writer.flush()?;
        if quitting {
            return Ok(());
        }
    }
}

fn execute(database: &Database, command: Vec<Vec<u8>>, out: &mut impl Write) -> io::Result<()> {
    if command.is_empty() {
        return write_error(out, "empty command");
    }
    let name = String::from_utf8_lossy(&command[0]).to_ascii_uppercase();
    match name.as_str() {
        "PING" if command.len() == 1 => write_simple(out, "PONG"),
        "PING" if command.len() == 2 => write_bulk(out, Some(&command[1])),
        "GET" if command.len() == 2 => write_bulk(out, database.get(&command[1]).as_deref()),
        "SET" => execute_set(database, &command, out),
        "DEL" | "DELETE" if command.len() >= 2 => {
            match database.delete(&command[1..]) {
                Ok(count) => write_integer(out, count),
                Err(error) => write_error(out, &format!("persistence failed: {error}")),
            }
        }
        "COMMAND" => write_array(out, &[]), // Lets redis-cli complete its initial handshake.
        "QUIT" => write_simple(out, "OK"),
        _ => write_error(out, "unknown command or wrong number of arguments"),
    }
}

fn execute_set(database: &Database, command: &[Vec<u8>], out: &mut impl Write) -> io::Result<()> {
    if command.len() != 3 && command.len() != 5 {
        return write_error(out, "usage: SET key value [EX seconds|PX milliseconds]");
    }
    let expires_at_ms = if command.len() == 5 {
        let option = String::from_utf8_lossy(&command[3]).to_ascii_uppercase();
        let amount = match std::str::from_utf8(&command[4]).ok().and_then(|s| s.parse::<u64>().ok()) {
            Some(value) if value > 0 => value,
            _ => return write_error(out, "expiration must be a positive integer"),
        };
        let multiplier = match option.as_str() {
            "EX" => 1_000,
            "PX" => 1,
            _ => return write_error(out, "SET supports only EX or PX"),
        };
        Some(now_ms().saturating_add(amount.saturating_mul(multiplier)))
    } else {
        None
    };

    match database.set(command[1].clone(), command[2].clone(), expires_at_ms) {
        Ok(()) => write_simple(out, "OK"),
        Err(error) => write_error(out, &format!("persistence failed: {error}")),
    }
}

fn read_command(reader: &mut impl BufRead) -> io::Result<Option<Vec<Vec<u8>>>> {
    let first = match read_line(reader)? {
        Some(line) => line,
        None => return Ok(None),
    };
    if first.first() != Some(&b'*') {
        return Ok(Some(first.split(|byte| byte.is_ascii_whitespace()).filter(|s| !s.is_empty()).map(Vec::from).collect()));
    }
    let count = parse_number(&first[1..])?;
    let mut parts = Vec::with_capacity(count);
    for _ in 0..count {
        let header = read_line(reader)?.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing bulk string"))?;
        if header.first() != Some(&b'$') {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "expected bulk string"));
        }
        let length = parse_number(&header[1..])?;
        let mut value = vec![0; length];
        reader.read_exact(&mut value)?;
        let mut ending = [0; 2];
        reader.read_exact(&mut ending)?;
        if ending != *b"\r\n" {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bulk string missing CRLF"));
        }
        parts.push(value);
    }
    Ok(Some(parts))
}

fn read_line(reader: &mut impl BufRead) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line)? == 0 {
        return Ok(None);
    }
    if line.ends_with(b"\n") { line.pop(); }
    if line.ends_with(b"\r") { line.pop(); }
    Ok(Some(line))
}

fn parse_number(bytes: &[u8]) -> io::Result<usize> {
    std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid length"))
}

fn write_simple(out: &mut impl Write, value: &str) -> io::Result<()> { write!(out, "+{value}\r\n") }
fn write_error(out: &mut impl Write, value: &str) -> io::Result<()> { write!(out, "-ERR {value}\r\n") }
fn write_integer(out: &mut impl Write, value: usize) -> io::Result<()> { write!(out, ":{value}\r\n") }
fn write_bulk(out: &mut impl Write, value: Option<&[u8]>) -> io::Result<()> {
    match value {
        Some(bytes) => { write!(out, "${}\r\n", bytes.len())?; out.write_all(bytes)?; out.write_all(b"\r\n") }
        None => out.write_all(b"$-1\r\n"),
    }
}
fn write_array(out: &mut impl Write, values: &[Vec<u8>]) -> io::Result<()> {
    write!(out, "*{}\r\n", values.len())?;
    for value in values { write_bulk(out, Some(value))?; }
    Ok(())
}

fn append_set(file: &mut File, key: &[u8], value: &[u8], expiry: Option<u64>) -> io::Result<()> {
    file.write_all(&[1])?;
    write_blob(file, key)?;
    write_blob(file, value)?;
    file.write_all(&expiry.unwrap_or(0).to_be_bytes())?;
    file.sync_data()
}

fn append_delete(file: &mut File, keys: &[Vec<u8>]) -> io::Result<()> {
    file.write_all(&[2])?;
    file.write_all(&(keys.len() as u32).to_be_bytes())?;
    for key in keys { write_blob(file, key)?; }
    file.sync_data()
}

fn write_blob(out: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    out.write_all(&(bytes.len() as u32).to_be_bytes())?;
    out.write_all(bytes)
}

fn replay(mut input: impl Read, entries: &mut HashMap<Vec<u8>, Entry>) -> io::Result<()> {
    loop {
        let mut kind = [0];
        match input.read_exact(&mut kind) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        }
        match kind[0] {
            1 => {
                let key = read_blob(&mut input)?;
                let value = read_blob(&mut input)?;
                let mut expiry = [0; 8];
                input.read_exact(&mut expiry)?;
                let expiry = u64::from_be_bytes(expiry);
                entries.insert(key, Entry { value, expires_at_ms: (expiry != 0).then_some(expiry) });
            }
            2 => {
                let mut count = [0; 4];
                input.read_exact(&mut count)?;
                for _ in 0..u32::from_be_bytes(count) { entries.remove(read_blob(&mut input)?.as_slice()); }
            }
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid AOF record")),
        }
    }
}

fn read_blob(input: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut length = [0; 4];
    input.read_exact(&mut length)?;
    let mut bytes = vec![0; u32::from_be_bytes(length) as usize];
    input.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64
}

fn expired(entry: &Entry) -> bool { entry.expires_at_ms.is_some_and(|expiry| expiry <= now_ms()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_resp_and_inline_commands() {
        let mut resp = &b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"[..];
        assert_eq!(
            read_command(&mut resp).unwrap().unwrap(),
            vec![b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()]
        );
        let mut inline = &b"GET key\r\n"[..];
        assert_eq!(
            read_command(&mut inline).unwrap().unwrap(),
            vec![b"GET".to_vec(), b"key".to_vec()]
        );
    }

    #[test]
    fn persistence_round_trip() {
        let mut path = std::env::temp_dir();
        path.push(format!("rusty-cache-test-{}-{}.aof", std::process::id(), now_ms()));
        {
            let db = Database::open(&path).unwrap();
            db.set(b"hello".to_vec(), b"world".to_vec(), None).unwrap();
            db.set(b"gone".to_vec(), b"soon".to_vec(), None).unwrap();
            db.delete(&[b"gone".to_vec()]).unwrap();
        }
        let restored = Database::open(&path).unwrap();
        assert_eq!(restored.get(b"hello"), Some(b"world".to_vec()));
        assert_eq!(restored.get(b"gone"), None);
        std::fs::remove_file(path).unwrap();
    }
}
