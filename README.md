This repository contains coursework from Carnegie Mellon University's 15-440 Distributed Systems class (Fall 2024), implemented in Go. Each project explores a foundational concept in building real-world distributed systems—from reliable communication to consensus and actor-based architectures.

## 🔧 P0: Go Concurrency + Key-Value Server

**Goal:** Learn Go’s concurrency model and testing framework by implementing:

- 🧩 **Part A:** A simple Key-Value server with concurrent client handling
- 🧪 **Part B:** Unit testing for a squarer module

### Features

- Uses Go's `goroutines` and `channels` to handle concurrency

### Bonus tools
- `srunner`: starts your key-value server on a default port
- Optional `crunner` or `netcat` to send/receive test messages


## 📡 P1: Reliable Communication (LSP) + Bitcoin Miner
**Goal:** Implement a custom protocol (LSP) on top of UDP, and build a distributed miner system with client/server.

### Components
- **Client:** sends hash task
- **Server:** distributes tasks to miners
- **Miner:** computes valid SHA-256 nonce

### Tests & Utilities
Use `srunner` and `crunner` to test early LSP implementations


## ⚖️ P2: Raft Consensus Protocol
**Goal:** Build a crash-tolerant, replicated state machine using the Raft protocol.

### Features
- Leader election
- Log replication
- Commit index tracking

## 🧠 P3: Actor-Based Key-Value Store (CMUD Game)
**Goal:** Build a fault-tolerant distributed key-value store using the actor model, enabling local and remote message passing.

### System Design
- `kvserver`: hosts request actors
- `kvclient`: communicates with actors via `tell`
- `app/`: CMUD game client to test your store
- `srunner` / `crunner`: runner tools for quick testing

## 💻 Tech Stack

- **Language:** Go (Golang) 1.23  
- **Concurrency & Threads:** Goroutines, Channels, Epoch-based timeouts  
- **Networking:** UDP Sockets, Custom LSP Protocol, RPC (used in Raft & Actor model)  
- **Distributed Algorithms:**  
  - P1: Lightweight Session Protocol (LSP) for reliable communication  
  - P2: Raft Consensus Protocol for leader election and log replication  
  - P3: Actor Model for distributed key-value store coordination  
- **CLI Tools & Utilities:** Netcat (`nc`), custom `srunner` / `crunner`, `cmud` interactive game client  
- **Documentation:** `godoc` for local API browsing  
- **Deployment / Environment:** Local Linux/macOS dev, AFS cluster machines (CMU), WSL2 for Windows


## 👩‍💻 Author
Hazel Li

