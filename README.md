# Rusty Cache

A small Redis-like in-memory database built with only Rust's standard library. It is deliberately compact enough to study while still including TCP networking, a real wire protocol, concurrent clients, expiration, and disk persistence.

## Features

- Redis RESP2 protocol plus simple inline commands
- `GET key`
- `SET key value [EX seconds|PX milliseconds]`
- `DEL key [key ...]` and `DELETE key [key ...]`
- `PING` and `QUIT`
- One thread per TCP client
- `Arc<RwLock<HashMap<...>>>` shared state
- Lazy expiration on reads plus a background expiry sweep
- Binary append-only persistence with absolute expiration timestamps
- Binary-safe keys and values when using RESP2
- No third-party crates

## Run

Install a current stable Rust toolchain, then:

```sh
cargo run
```

The server listens on `127.0.0.1:6379` and writes `rusty-cache.aof` in the current directory. Both are configurable:

```sh
RUSTY_CACHE_ADDR=0.0.0.0:6380 RUSTY_CACHE_FILE=cache.aof cargo run
```

Use `redis-cli`:

```console
$ redis-cli SET greeting hello EX 60
OK
$ redis-cli GET greeting
"hello"
$ redis-cli DEL greeting
(integer) 1
```

Or use netcat with the simpler inline protocol:

```sh
printf 'SET name ferris\r\nGET name\r\n' | nc 127.0.0.1 6379
```

Run tests with `cargo test`.

## Design

Each accepted connection gets a dedicated thread. All client threads share the database through an `Arc`; readers take a shared read lock, while `SET`, `DEL`, and expiration cleanup take a write lock. Mutations are appended and synced to the AOF before becoming visible in memory, preventing an acknowledged write from being lost after a normal restart.

The persistence file is replayed at startup. A `SET` record stores its absolute Unix-millisecond expiry, so restarting the process never resets a key's TTL. Expired records can remain in the append-only file and are discarded during startup; a production evolution would add periodic AOF compaction.

## Intentional limits

This is a learning project, not a production Redis replacement. It has no authentication, TLS, replication, transactions, eviction policy, AOF compaction, pipelined worker pool, or maximum request-size guard. The thread-per-connection model is approachable but should evolve to an async runtime or bounded pool for very large client counts.
