# Buffered network reads

`BufReader` avoids one system call per byte while RESP headers are read up to their CRLF terminator.

Follow the flow in `src/main.rs` and `src/lib.rs`, then change one behavior and observe the client-visible result. This keeps the lesson grounded in executable code.

