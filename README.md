# Two Phase Commit (2PC Protocol)
This is a basic implementation of the 2PC protocol in Rust.

Build and run:

```sh
cargo build
./target/debug/two_phase_commit -s .95 -c 4 -p 10 -r 10 -m run
```

The coordinator starts the client and participant processes and handles one
transaction at a time. Each client sends a request and waits for its result.
Participants log their votes before sending them. 

After the requests finish, the processes wait for Ctrl+C. On Ctrl+C, the
coordinator finishes or aborts the active transaction and sends shutdown messages.
Children process preceding decisions before reporting their counts and exiting.
`U` counts requests or proposals whose final outcome was not received.

Run it in check mode:
```sh
./target/debug/two_phase_commit -s .95 -c 4 -p 10 -r 10 -m check -v 0
```

