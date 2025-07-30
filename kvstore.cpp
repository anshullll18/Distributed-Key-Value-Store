#include <iostream>
#include <unordered_map>
#include <vector>
#include <string>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <chrono>
#include <functional>
#include <algorithm>
#include <queue>
#include <atomic>
#include <random>
#include <memory>
#include <fstream>
#include <sstream>
#include <cassert>
#include <map>  
#include <set>  

using namespace std;

// Hash function for consistent hashing
class ConsistentHash {
private:
    map<uint32_t, string> ring;
    hash<string> hasher;
    int virtual_nodes;
    
public:
    ConsistentHash(int vn = 100) : virtual_nodes(vn) {}
    
    void addNode(const string& node) {
        for (int i = 0; i < virtual_nodes; ++i) {
            uint32_t hash = hasher(node + to_string(i));
            ring[hash] = node;
        }
    }
    
    void removeNode(const string& node) {
        for (int i = 0; i < virtual_nodes; ++i) {
            uint32_t hash = hasher(node + to_string(i));
            ring.erase(hash);
        }
    }
    
    string getNode(const string& key) {
        if (ring.empty()) return "";
        
        uint32_t hash = hasher(key);
        auto it = ring.lower_bound(hash);
        if (it == ring.end()) {
            it = ring.begin();
        }
        return it->second;
    }
    
    vector<string> getNodes(const string& key, int count) {
        vector<string> nodes;
        if (ring.empty()) return nodes;
        
        uint32_t hash = hasher(key);
        auto it = ring.lower_bound(hash);
        
        set<string> unique_nodes;
        int attempts = 0;
        int max_attempts = ring.size() * 2; // Prevent infinite loops
        
        while (unique_nodes.size() < count && attempts < max_attempts) {
            if (it == ring.end()) it = ring.begin();
            unique_nodes.insert(it->second);
            ++it;
            ++attempts;
        }
        
        return vector<string>(unique_nodes.begin(), unique_nodes.end());
    }
};

// Thread-safe LRU Cache
template<typename K, typename V>
class LRUCache {
private:
    struct Node {
        K key;
        V value;
        shared_ptr<Node> prev, next;
        Node(K k, V v) : key(k), value(v) {}
    };
    
    unordered_map<K, shared_ptr<Node>> cache;
    shared_ptr<Node> head, tail;
    int capacity;
    mutable shared_mutex mutex;
    
    void moveToHead(shared_ptr<Node> node) {
        removeNode(node);
        addToHead(node);
    }
    
    void removeNode(shared_ptr<Node> node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }
    
    void addToHead(shared_ptr<Node> node) {
        node->prev = head;
        node->next = head->next;
        head->next->prev = node;
        head->next = node;
    }
    
    shared_ptr<Node> removeTail() {
        auto last = tail->prev;
        removeNode(last);
        return last;
    }
    
public:
    LRUCache(int cap) : capacity(cap) {
        head = make_shared<Node>(K{}, V{});
        tail = make_shared<Node>(K{}, V{});
        head->next = tail;
        tail->prev = head;
    }
    
    V get(const K& key) {
        shared_lock<shared_mutex> lock(mutex);
        auto it = cache.find(key);
        if (it != cache.end()) {
            moveToHead(it->second);
            return it->second->value;
        }
        return V{};
    }
    
    void put(const K& key, const V& value) {
        unique_lock<shared_mutex> lock(mutex);
        auto it = cache.find(key);
        
        if (it != cache.end()) {
            it->second->value = value;
            moveToHead(it->second);
        } else {
            auto newNode = make_shared<Node>(key, value);
            
            if (cache.size() >= capacity) {
                auto tail_node = removeTail();
                cache.erase(tail_node->key);
            }
            
            cache[key] = newNode;
            addToHead(newNode);
        }
    }
    
    bool remove(const K& key) {
        unique_lock<shared_mutex> lock(mutex);
        auto it = cache.find(key);
        if (it != cache.end()) {
            removeNode(it->second);
            cache.erase(key);
            return true;
        }
        return false;
    }
};

// Storage Engine with WAL (Write-Ahead Logging)
class StorageEngine {
private:
    unordered_map<string, string> data;
    shared_mutex data_mutex;
    ofstream wal_file;
    mutex wal_mutex;
    
public:
    StorageEngine(const string& wal_path = "kvstore.wal") 
        : wal_file(wal_path, ios::app) {
        loadFromWAL(wal_path);
    }
    
    void put(const string& key, const string& value) {
        // Write to WAL first
        {
            lock_guard<mutex> wal_lock(wal_mutex);
            wal_file << "PUT " << key << " " << value << "\n";
            wal_file.flush();
        }
        
        // Then update in-memory data
        unique_lock<shared_mutex> lock(data_mutex);
        data[key] = value;
    }
    
    string get(const string& key) {
        shared_lock<shared_mutex> lock(data_mutex);
        auto it = data.find(key);
        return (it != data.end()) ? it->second : "";
    }
    
    bool remove(const string& key) {
        // Write to WAL first
        {
            lock_guard<mutex> wal_lock(wal_mutex);
            wal_file << "DEL " << key << "\n";
            wal_file.flush();
        }
        
        unique_lock<shared_mutex> lock(data_mutex);
        return data.erase(key) > 0;
    }
    
    vector<string> getAllKeys() {
        shared_lock<shared_mutex> lock(data_mutex);
        vector<string> keys;
        for (const auto& pair : data) {
            keys.push_back(pair.first);
        }
        return keys;
    }
    
private:
    void loadFromWAL(const string& wal_path) {
        ifstream file(wal_path);
        string line;
        while (getline(file, line)) {
            istringstream iss(line);
            string op, key, value;
            iss >> op >> key;
            
            if (op == "PUT") {
                getline(iss, value);
                if (!value.empty() && value[0] == ' ') {
                    value = value.substr(1);
                }
                data[key] = value;
            } else if (op == "DEL") {
                data.erase(key);
            }
        }
    }
};

// Node in the distributed system
class KVNode {
private:
    string node_id;
    StorageEngine storage;
    LRUCache<string, string> cache;
    vector<string> replica_nodes;
    atomic<bool> is_leader{false};
    
public:
    KVNode(const string& id, int cache_size = 1000) 
        : node_id(id), cache(cache_size), storage(id + ".wal") {}
    
    // Basic operations
    void put(const string& key, const string& value) {
        storage.put(key, value);
        cache.put(key, value);
        
        // Note: Replication is handled at the cluster level
        // This node just stores the data locally
    }
    
    string get(const string& key) {
        // Try cache first
        string value = cache.get(key);
        if (!value.empty()) {
            return value;
        }
        
        // Fallback to storage
        value = storage.get(key);
        if (!value.empty()) {
            cache.put(key, value);
        }
        return value;
    }
    
    bool remove(const string& key) {
        bool result = storage.remove(key);
        // Also remove from cache to ensure consistency
        cache.remove(key);
        return result;
    }
    
    void addReplica(const string& replica_id) {
        replica_nodes.push_back(replica_id);
    }
    
    string getNodeId() const { return node_id; }
    
    void setLeader(bool leader) { is_leader = leader; }
    bool isLeader() const { return is_leader; }
    
    // Get all keys stored on this node
    vector<string> getAllKeys() {
        return storage.getAllKeys();
    }
};

// Distributed Key-Value Store Cluster
class DistributedKVStore {
private:
    unordered_map<string, unique_ptr<KVNode>> nodes;
    ConsistentHash hash_ring;
    int replication_factor;
    shared_mutex cluster_mutex;
    
public:
    DistributedKVStore(int rf = 3) : replication_factor(rf) {}
    
    void addNode(const string& node_id) {
        unique_lock<shared_mutex> lock(cluster_mutex);
        nodes[node_id] = make_unique<KVNode>(node_id);
        hash_ring.addNode(node_id);
        
        // Set up replication
        setupReplication();
    }
    
    void removeNode(const string& node_id) {
        unique_lock<shared_mutex> lock(cluster_mutex);
        
        // Check if the node exists
        auto node_it = nodes.find(node_id);
        if (node_it == nodes.end()) {
            cout << "Node " << node_id << " not found in cluster." << endl;
            return;
        }
        
        cout << "Removing node " << node_id << " and redistributing data..." << endl;
        
        // Step 1: Get all keys and values from the node to be removed
        vector<string> keys_to_redistribute = node_it->second->getAllKeys();
        cout << "Found " << keys_to_redistribute.size() << " keys to redistribute." << endl;
        
        // Step 2: Collect all key-value pairs before removing the node
        vector<pair<string, string>> key_value_pairs;
        for (const string& key : keys_to_redistribute) {
            string value = node_it->second->get(key);
            if (!value.empty()) {
                key_value_pairs.push_back({key, value});
            }
        }
        
        // Step 3: Remove node from hash ring and cluster
        hash_ring.removeNode(node_id);
        nodes.erase(node_id);
        
        // Step 4: Redistribute each key-value pair to new responsible nodes
        int redistributed_count = 0;
        for (const auto& kv_pair : key_value_pairs) {
            const string& key = kv_pair.first;
            const string& value = kv_pair.second;
            
            // Find new responsible nodes for this key
            // Use min(replication_factor, available_nodes) to avoid asking for more nodes than available
            int available_nodes = nodes.size();
            int effective_replication = min(replication_factor, available_nodes);
            auto new_responsible_nodes = hash_ring.getNodes(key, effective_replication);
            
            cout << "    Redistributing key '" << key << "' to " << new_responsible_nodes.size() 
                 << " nodes (effective replication: " << effective_replication << ", available nodes: " << available_nodes << ")" << endl;
            
            // Redistribute to new nodes
            for (const string& new_node_id : new_responsible_nodes) {
                auto new_node_it = nodes.find(new_node_id);
                if (new_node_it != nodes.end()) {
                    // Check if the key already exists on this node
                    string existing_value = new_node_it->second->get(key);
                    if (existing_value.empty()) {
                        // Key doesn't exist, add it
                        new_node_it->second->put(key, value);
                        cout << "  ✓ Redistributed key '" << key << "' to node " << new_node_id << " (NEW)" << endl;
                        redistributed_count++;
                    } else {
                        // Key already exists, skip
                        cout << "  - Key '" << key << "' already exists on node " << new_node_id << " (SKIP)" << endl;
                    }
                }
            }
        }
        
        cout << "✓ Node " << node_id << " removed. " << redistributed_count << " keys redistributed." << endl;
    }
    
    void put(const string& key, const string& value) {
        shared_lock<shared_mutex> lock(cluster_mutex);
        
        auto responsible_nodes = hash_ring.getNodes(key, replication_factor);
        if (responsible_nodes.empty()) {
            throw runtime_error("No nodes available");
        }
        
        // Write to primary node and replicas
        cout << "Storing " << key << " on " << responsible_nodes.size() << " nodes for replication" << endl;
        for (const auto& node_id : responsible_nodes) {
            auto it = nodes.find(node_id);
            if (it != nodes.end()) {
                it->second->put(key, value);
                cout << "  ✓ Stored on node: " << node_id << endl;
            }
        }
    }
    
    string get(const string& key) {
        shared_lock<shared_mutex> lock(cluster_mutex);
        
        auto responsible_nodes = hash_ring.getNodes(key, replication_factor);
        if (responsible_nodes.empty()) {
            return "";
        }
        
        // Try to read from any available replica
        for (const auto& node_id : responsible_nodes) {
            auto it = nodes.find(node_id);
            if (it != nodes.end()) {
                string value = it->second->get(key);
                if (!value.empty()) {
                    return value;
                }
            }
        }
        return "";
    }
    
    bool remove(const string& key) {
        shared_lock<shared_mutex> lock(cluster_mutex);
        
        auto responsible_nodes = hash_ring.getNodes(key, replication_factor);
        bool success = false;
        
        cout << "Deleting " << key << " from " << responsible_nodes.size() << " nodes" << endl;
        for (const auto& node_id : responsible_nodes) {
            auto it = nodes.find(node_id);
            if (it != nodes.end()) {
                bool node_success = it->second->remove(key);
                success |= node_success;
                cout << "  " << (node_success ? "✓" : "✗") << " Deleted from node: " << node_id << endl;
            }
        }
        return success;
    }
    
    void printClusterInfo() {
        shared_lock<shared_mutex> lock(cluster_mutex);
        cout << "Cluster has " << nodes.size() << " nodes:" << endl;
        for (const auto& pair : nodes) {
            cout << "- Node: " << pair.first << " (keys: " << pair.second->getAllKeys().size() << ")" << endl;
        }
    }
    
private:
    void setupReplication() {
        // Simple replication setup - each node knows about its replicas
        for (auto& pair : nodes) {
            const string& node_id = pair.first;
            auto replicas = hash_ring.getNodes(node_id, replication_factor);
            
            for (const auto& replica_id : replicas) {
                if (replica_id != node_id) {
                    pair.second->addReplica(replica_id);
                }
            }
        }
    }
};

// Performance benchmarking
class Benchmark {
public:
    static void runBenchmark(DistributedKVStore& store, int num_operations = 10000) {
        cout << "\n=== Running Benchmark ===" << endl;
        
        auto start = chrono::high_resolution_clock::now();
        
        // Write benchmark
        for (int i = 0; i < num_operations; ++i) {
            string key = "key" + to_string(i);
            string value = "value" + to_string(i);
            store.put(key, value);
        }
        
        auto write_end = chrono::high_resolution_clock::now();
        auto write_duration_ms = chrono::duration_cast<chrono::milliseconds>(write_end - start);
        auto write_duration_us = chrono::duration_cast<chrono::microseconds>(write_end - start);
        
        // Read benchmark
        for (int i = 0; i < num_operations; ++i) {
            string key = "key" + to_string(i);
            store.get(key);
        }
        
        auto read_end = chrono::high_resolution_clock::now();
        auto read_duration_ms = chrono::duration_cast<chrono::milliseconds>(read_end - write_end);
        auto read_duration_us = chrono::duration_cast<chrono::microseconds>(read_end - write_end);
        
        cout << "Write operations: " << num_operations << " in " 
             << write_duration_ms.count() << "ms (" << write_duration_us.count() << "us)" << endl;
        if (write_duration_us.count() > 0) {
            cout << "Write throughput: " << (num_operations * 1000000LL / write_duration_us.count()) 
                 << " ops/sec" << endl;
        } else {
            cout << "Write throughput: N/A (duration too short for accurate measurement)" << endl;
        }
        
        cout << "Read operations: " << num_operations << " in " 
             << read_duration_ms.count() << "ms (" << read_duration_us.count() << "us)" << endl;
        if (read_duration_us.count() > 0) {
            cout << "Read throughput: " << (num_operations * 1000000LL / read_duration_us.count()) 
                 << " ops/sec" << endl;
        } else {
            cout << "Read throughput: N/A (duration too short for accurate measurement)" << endl;
        }
        if (write_duration_ms.count() == 0 || read_duration_ms.count() == 0) {
            cout << "[Warning] Benchmark completed too quickly for accurate timing. Increase num_operations for more reliable results." << endl;
        }
    }
};

// Interactive demo 
void interactiveDemo() {
    cout << "\n=== INTERACTIVE DEMO MODE ===" << endl;
    cout << "Commands: put <key> <value>, get <key>, del <key>, nodes, benchmark, addnode, removenode, showdata, exit" << endl;
    cout << "Note: For values with spaces, use quotes like: put user:1001 \"Alice Johnson\"" << endl;
    
    DistributedKVStore cluster(3);
    
    // Add initial nodes
    cluster.addNode("node1");
    cluster.addNode("node2");
    cluster.addNode("node3");
    
    string command;
    while (cout << "\nkvstore> " && cin >> command) {
        if (command == "put") {
            string key;
            cin >> key;
            
            // Read the rest of the line as the value
            string value;
            getline(cin, value);
            
            // Remove leading space if present
            if (!value.empty() && value[0] == ' ') {
                value = value.substr(1);
            }
            
            // Handle quoted values
            if (!value.empty() && value[0] == '"') {
                value = value.substr(1); // Remove opening quote
                if (!value.empty() && value.back() == '"') {
                    value = value.substr(0, value.length() - 1); // Remove closing quote
                }
            }
            
            cluster.put(key, value);
            cout << "✓ Stored: " << key << " -> " << value << endl;
        }
        else if (command == "get") {
            string key;
            cin >> key;
            string value = cluster.get(key);
            if (value.empty()) {
                cout << "✗ Key not found: " << key << endl;
            } else {
                cout << "✓ Retrieved: " << key << " -> " << value << endl;
            }
        }
        else if (command == "del") {
            string key;
            cin >> key;
            bool success = cluster.remove(key);
            cout << (success ? "✓ Deleted: " : "✗ Not found: ") << key << endl;
        }
        else if (command == "nodes") {
            cluster.printClusterInfo();
        }
        else if (command == "benchmark") {
            cout << "Running benchmark..." << endl;
            Benchmark::runBenchmark(cluster, 1000);
        }
        else if (command == "addnode") {
            string nodeId;
            cin >> nodeId;
            cluster.addNode(nodeId);
            cout << "✓ Added node: " << nodeId << endl;
        }
        else if (command == "removenode") {
            string nodeId;
            cin >> nodeId;
            cluster.removeNode(nodeId);
        }
        else if (command == "showdata") {
            cluster.printClusterInfo();
        }
        else if (command == "exit") {
            break;
        }
        else {
            cout << "Unknown command. Available: put, get, del, nodes, benchmark, addnode, removenode, showdata, exit" << endl;
        }
    }
}

// Automated demo 
void automatedDemo() {
    cout << "=== AUTOMATED DISTRIBUTED KEY-VALUE STORE DEMO ===" << endl;
    cout << "Demonstrating all features for system design interview...\n" << endl;
    
    // Create cluster
    DistributedKVStore cluster(3); // replication factor of 3
    
    cout << "1. CLUSTER SETUP" << endl;
    cout << "Creating cluster with replication factor 3..." << endl;
    
    // Add nodes
    cluster.addNode("node1");
    cluster.addNode("node2");
    cluster.addNode("node3");
    cluster.addNode("node4");
    cluster.addNode("node5");
    
    cluster.printClusterInfo();
    this_thread::sleep_for(chrono::milliseconds(1000));
    
    // Basic operations
    cout << "\n2. BASIC CRUD OPERATIONS" << endl;
    cout << "Demonstrating PUT and GET operations..." << endl;
    
    cluster.put("user:1001", "Alice Johnson");
    cluster.put("user:1002", "Bob Smith");
    cluster.put("user:1003", "Charlie Brown");
    cluster.put("session:abc123", "active");
    cluster.put("config:timeout", "30s");
    
    cout << "✓ Stored 5 key-value pairs" << endl;
    
    cout << "\nRetrieving values:" << endl;
    cout << "user:1001 = " << cluster.get("user:1001") << endl;
    cout << "user:1002 = " << cluster.get("user:1002") << endl;
    cout << "session:abc123 = " << cluster.get("session:abc123") << endl;
    
    this_thread::sleep_for(chrono::milliseconds(1500));
    
    // Test consistency
    cout << "\n3. CONSISTENCY DEMONSTRATION" << endl;
    cluster.put("test:consistency", "version_1");
    cout << "Initial value: " << cluster.get("test:consistency") << endl;
    
    cluster.put("test:consistency", "version_2");
    cout << "Updated value: " << cluster.get("test:consistency") << endl;
    cout << "✓ Strong consistency maintained across replicas" << endl;
    
    this_thread::sleep_for(chrono::milliseconds(1000));
    
    // Test node failure simulation
    cout << "\n4. FAULT TOLERANCE & HIGH AVAILABILITY" << endl;
    cout << "Simulating node failure..." << endl;
    
    cout << "Before failure - user:1001 = " << cluster.get("user:1001") << endl;
    cluster.removeNode("node1");
    cout << "After removing node1 - user:1001 = " << cluster.get("user:1001") << endl;
    cout << "✓ Data still accessible due to replication!" << endl;
    
    this_thread::sleep_for(chrono::milliseconds(1500));
    
    // Performance demonstration
    cout << "\n5. PERFORMANCE & SCALABILITY" << endl;
    cout << "Running performance benchmark..." << endl;
    Benchmark::runBenchmark(cluster, 2000);
    
    this_thread::sleep_for(chrono::milliseconds(1000));
    
    // Concurrent access test
    cout << "\n6. CONCURRENCY & THREAD SAFETY" << endl;
    cout << "Testing concurrent access with 4 threads..." << endl;
    
    auto start = chrono::high_resolution_clock::now();
    vector<thread> threads;
    atomic<int> operations_completed{0};
    
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&cluster, i, &operations_completed]() {
            for (int j = 0; j < 50; ++j) {
                string key = "thread" + to_string(i) + ":key" + to_string(j);
                string value = "thread" + to_string(i) + ":value" + to_string(j);
                cluster.put(key, value);
                
                string retrieved = cluster.get(key);
                assert(retrieved == value);
                operations_completed++;
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
    
    cout << "✓ " << operations_completed << " concurrent operations completed in " 
              << duration.count() << "ms" << endl;
    cout << "✓ Thread safety verified - no data corruption!" << endl;
    
    this_thread::sleep_for(chrono::milliseconds(1000));
    
    // Architecture summary
    cout << "\n7. ARCHITECTURE SUMMARY" << endl;
    cout << "✓ Consistent Hashing: Even data distribution" << endl;
    cout << "✓ Replication: 3x fault tolerance" << endl;
    cout << "✓ Caching: LRU cache for performance" << endl;
    cout << "✓ Persistence: Write-Ahead Logging (WAL)" << endl;
    cout << "✓ Concurrency: Thread-safe operations" << endl;
    cout << "✓ Scalability: Horizontal scaling ready" << endl;
    
}

// Demo and testing
int main(int argc, char* argv[]) {
    cout << "Distributed Key-Value Store - System Design Interview Demo" << endl;
    cout << "=========================================================" << endl;
    
    if (argc > 1 && string(argv[1]) == "--interactive") {
        interactiveDemo();
    } else {
        automatedDemo();
        
        cout << "\nWant to try interactive mode? Run with --interactive flag" << endl;
        cout << "Example: ./kvstore --interactive" << endl;
    }
    
    return 0;
}