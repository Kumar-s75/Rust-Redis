# Cloning TCP streams

A cloned socket handle lets `BufReader` own the read side while the original handle writes responses to the same connection.

Follow the flow in `src/main.rs` and `src/lib.rs`, then change one behavior and observe the client-visible result. This keeps the lesson grounded in executable code.

