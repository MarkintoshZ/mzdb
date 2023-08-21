# MZDB

A toy project using distributed hash table (DHT) for fun stuff

## Feature Goals

- [x] Get, Set Basic operations
- [-] Distributed via [Chord Protocol](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf)
  - [x] Distributed key lookup and write operations
  - [ ] Failure detection and recovery
- [ ] WASM modules to support custom databse operations

## Get Started

### Start Servers using tmux

1. Create a tmux window with 3 panels
2. Run in the first panel `cargo b && ./run.sh`

### Connect to server node

Run `cargo r --bin mzdb_client "[::]:8000"`
