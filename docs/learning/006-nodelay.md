# TCP_NODELAY behavior

The client handler enables `TCP_NODELAY` to avoid delaying the small request-response messages typical of a key-value server.

Follow the flow in `src/main.rs` and `src/lib.rs`, then change one behavior and observe the client-visible result. This keeps the lesson grounded in executable code.

