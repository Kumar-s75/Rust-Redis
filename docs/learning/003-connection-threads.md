# Thread-per-connection model

Each accepted stream receives an owned thread and a cloned `Arc`, making client isolation easy to trace.

Follow the flow in `src/main.rs` and `src/lib.rs`, then change one behavior and observe the client-visible result. This keeps the lesson grounded in executable code.

