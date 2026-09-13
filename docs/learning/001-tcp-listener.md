# TCP listener lifecycle

Accepting a socket is the boundary between the listening thread and a client worker; `src/main.rs` keeps that responsibility explicit.

Follow the flow in `src/main.rs` and `src/lib.rs`, then change one behavior and observe the client-visible result. This keeps the lesson grounded in executable code.

