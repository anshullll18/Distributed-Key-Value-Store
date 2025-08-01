# Distributed Key-Value Store with Smart Redistribution

A high-performance, fault-tolerant distributed key-value store implementation in C++ featuring consistent hashing, smart data redistribution, and enterprise-grade reliability features.

## 🚀 Features

### Core Architecture
- **Consistent Hashing**: Minimal data movement (O(K/N) keys) during scaling operations
- **Smart Redistribution**: Only affected keys are moved when nodes are added/removed
- **Multi-layered Storage**: LRU cache + persistent storage with WAL (Write-Ahead Logging)
- **Thread-Safe Operations**: Full concurrent read/write support with shared_mutex
- **Fault Tolerance**: 3x replication factor for high availability

### Advanced Capabilities
- **Zero-Downtime Scaling**: Add/remove nodes without service interruption
- **Batch Operations**: Efficient bulk data transfer during redistribution
- **Crash Recovery**: Automatic recovery from WAL logs
- **Performance Benchmarking**: Built-in throughput and latency testing
- **Interactive CLI**: Real-time cluster management and data operations

## 📋 Prerequisites

- C++17 or higher
- GCC or Clang compiler
- Make (optional, for build automation)

## 🔧 Compilation

### Basic Compilation
```bash
g++ -std=c++17 -pthread -O2 kvstore.cpp -o kvstore
```

### With Debug Information
```bash
g++ -std=c++17 -pthread -g -DDEBUG kvstore.cpp -o kvstore
```

### Optimized Release Build
```bash
g++ -std=c++17 -pthread -O3 -DNDEBUG kvstore.cpp -o kvstore
```

## 🏃 Running the Application

### Automated Demo Mode (Default)
```bash
./kvstore
```
Runs a comprehensive demo showcasing all features including:
- Cluster initialization
- Data population
- Smart redistribution
- Node scaling
- Concurrent operations
- Performance benchmarks

### Interactive Mode
```bash
./kvstore --interactive
```
Launches an interactive CLI for real-time cluster management.

## 📝 Interactive Commands

### Data Operations
```bash
# Store a key-value pair
put <key> <value>
put user:1001 "Alice Johnson"
put session:abc123 active

# Retrieve a value by key
get <key>
get user:1001

# Delete a key
del <key>
del session:abc123
```

### Cluster Management
```bash
# Add a new node to the cluster
addnode <node_id>
addnode node4

# Remove a node from the cluster
removenode <node_id>
removenode node2

# Display all nodes in the cluster
nodes

# Show data distribution statistics
stats
```

### Performance & Utilities
```bash
# Run performance benchmark
benchmark

# Exit interactive mode
exit
```

## 🔍 Example Interactive Session

```bash
$ ./kvstore --interactive

=== INTERACTIVE DEMO MODE ===
Commands: put <key> <value>, get <key>, del <key>, nodes, benchmark, addnode <id>, removenode <id>, stats, exit

kvstore> put user:1001 "Alice Johnson"
✓ Stored: user:1001 -> Alice Johnson

kvstore> put user:1002 "Bob Smith"
✓ Stored: user:1002 -> Bob Smith

kvstore> get user:1001
✓ Retrieved: user:1001 -> Alice Johnson

kvstore> nodes
Cluster has 3 nodes:
- Node: node1
- Node: node2
- Node: node3

kvstore> stats
=== Data Distribution Statistics ===
Node node1: 1 keys (50.0%)
Node node2: 1 keys (50.0%)
Node node3: 0 keys (0.0%)
Total keys in cluster: 2

kvstore> addnode node4

=== Adding Node: node4 ===
Performing smart redistribution for new node...
  Moving 1 keys from node1 to node4
✓ Redistribution complete: 1 keys moved
✓ Node node4 added successfully with minimal redistribution

kvstore> stats
=== Data Distribution Statistics ===
Node node1: 0 keys (0.0%)
Node node2: 1 keys (50.0%)
Node node3: 0 keys (0.0%)
Node node4: 1 keys (50.0%)
Total keys in cluster: 2

kvstore> benchmark
Running benchmark...
Write operations: 1000 in 15ms (15234us)
Write throughput: 65616 ops/sec
Read operations: 1000 in 8ms (8123us)  
Read throughput: 123067 ops/sec

kvstore> exit
```

## 🏗️ Architecture Overview

### Components

1. **ConsistentHash**: Implements consistent hashing with virtual nodes
2. **LRUCache**: Thread-safe LRU cache with shared_mutex
3. **StorageEngine**: Persistent storage with Write-Ahead Logging
4. **KVNode**: Individual node in the distributed system
5. **DistributedKVStore**: Main cluster coordinator
6. **Benchmark**: Performance testing utilities

### Data Flow

```
Client Request → Hash Ring → Primary Node → Replica Nodes
                     ↓
                LRU Cache → Storage Engine → WAL File
```

## 📊 Performance Characteristics

### Scaling Benefits
- **Traditional Hash Systems**: ~50% data movement on scaling
- **Our Consistent Hash**: Only ~1/N data movement per node
- **Zero Downtime**: Operations continue during redistribution
- **Linear Scalability**: Performance scales with node count

### Benchmarks (Typical Results)
- **Write Throughput**: 50,000+ ops/sec
- **Read Throughput**: 100,000+ ops/sec  
- **Latency**: Sub-millisecond for cached reads
- **Concurrent Operations**: Thousands of simultaneous requests

## 🗂️ File Structure

```
kvstore.cpp              # Main implementation file
<node_id>.wal           # Write-Ahead Log files (auto-generated)
├── node1.wal          # WAL for node1
├── node2.wal          # WAL for node2
└── ...                # Additional node WAL files
```

## 🔧 Configuration Options

### Replication Factor
```cpp
DistributedKVStore cluster(3);  // 3x replication
```

### Cache Size
```cpp
KVNode node("node1", 1000);    // 1000-entry LRU cache
```

### Virtual Nodes (Consistent Hashing)
```cpp
ConsistentHash hash_ring(100); // 100 virtual nodes per physical node
```

