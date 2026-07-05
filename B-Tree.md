## What is a B-Tree?
A B-tree is a self-balancing tree data structure that keeps data sorted and allows for fast searches, insertions, and deletions.
Unlike a standard binary tree where each node can only have two children, a B-tree node can have dozens or hundreds of children. Because it branches out so widely, the tree stays very short, making it highly efficient for reading and writing large blocks of data.

## Core Properties of a B-Tree
From a system design perspective, a B-Tree must follow a strict set of structural properties to remain perfectly balanced and optimized for hardware.
Every B-Tree is defined by its Order (m), which represents the maximum number of children any single node can have.

### 1. Node Capacity Properties
To keep the tree wide and flat, nodes are tightly regulated on how full or empty they can be:

* **Maximum Keys**: Any node can contain at most m - 1 keys.
* **Maximum Children**: Any internal node can have at most m child pointers.
* **Minimum Keys (Non-Root)**: Every node (except the root) must be at least half full. It must contain at least $\lceil m/2 \rceil - 1$ keys.
* **Minimum Children (Non-Root)**: Every internal node must have at least $\lceil m/2 \rceil$ children.
* **The Root Exception**: The root node is allowed to be mostly empty if the tree is small. It requires a minimum of just 1 key and 2 children (unless the entire tree consists only of the root node itself).

### 2. Structural & Balancing Properties
These properties ensure that data access times remain incredibly fast and predictable:

* **Perfect Height Balance**: All leaf nodes must reside at the exact same depth level. The tree never becomes lopsided like a standard binary tree. It grows and shrinks uniformly.
* **Key-to-Child Relationship**: If a node currently contains k keys, it must have exactly k + 1 child pointers.
* **Bottom-Up Growth**: Unlike most trees that grow downward by adding new leaf nodes at the bottom, a B-Tree grows upward. When a leaf node overflows, it splits and pushes a key up, eventually creating a new root when the top level overflows.


### 3. Data Ordering Properties
These rules enable the system to use fast binary search strategies inside each individual node:

* **Internal Node Sorting**: All keys within a single node are strictly maintained in ascending (sorted) order.
* **Subtree Boundaries**: The keys inside a node act as fences or boundaries for its children. For a node with keys $[K_1, K_2, \dots, K_n]$:
* The first child pointer leads to a subtree containing only values less than K₁.
   * The child pointer between $K_i$ and $K_{i+1}$ leads to a subtree containing values strictly between $K_i$ and $K_{i+1}$.
   * The final child pointer leads to a subtree containing only values greater than $K_n$.

## Architectural Summary

| Property | Value/Rule | Why It Matters in System Design |
|---|---|---|
| Max Capacity (m) | Chosen to match disk block size (e.g., 4KB) | Ensures loading a node takes exactly one disk read (I/O). |
| Min Capacity ($\lceil m/2 \rceil$) | Guarantees at least 50% storage utilization | Prevents massive amounts of wasted disk space. |
| Perfect Balance | All leaves at the same depth | Guarantees a strict $O(\log n)$ SLA; no query takes an unexpected hit. |

## Where is it Used?
B-trees (and their variants like B+ Trees) are the foundational backbone of data storage in modern software architecture:

* **Relational Databases (RDBMS)**: Engines like MySQL (InnoDB), PostgreSQL, and Oracle use them to build indexes so that queries take milliseconds instead of scanning the whole drive.
* **NoSQL Databases**: MongoDB and Couchbase use them for primary and secondary indexing.
* **File Systems**: Operating systems use them to track files on your drive. Examples include NTFS (Windows), HFS+ and APFS (macOS), and ext4 (Linux).

## Why is it Used?
Computer systems read and write data from physical drives (SSDs or Hard Drives) in large, fixed-size chunks called pages or blocks (usually 4KB or 8KB).

* **The Problem with Binary Trees:** Traditional binary search trees are deep and narrow. Searching millions of records requires jumping down many levels. On a disk, every single jump down a level requires a separate disk read operation (I/O), which is incredibly slow.
* **The B-Tree Solution:** A B-tree node is designed to match the exact size of a disk block. When you load one node, you load hundreds of keys into fast system memory (RAM) in one single disk read. Because the tree has a high branching factor, it can store billions of records in just 3 or 4 levels, ensuring stable, predictable latency.

## All Allowed Operations
Every operation in a B-tree runs in $O(\log n)$ time complexity. Here are the core actions the structure allows:
### 1. Search (Read)

* **Goal**: Find a specific key in the tree.
* **How it works**: The system starts at the root node and scans the sorted keys inside it. If it finds the key, it stops. If not, it uses the keys as boundaries to figure out which child pointer to follow. It moves down to that child node and repeats the process until the key is found or it hits a leaf node (meaning the key doesn't exist).

### 2. Insertion (Write)

* **Goal**: Add a new key while keeping the tree perfectly balanced.
* **How it works:** The tree always inserts new keys into a leaf node. The system searches down to find the correct leaf and places the key in sorted order.
* **Handling Overflow:** If the node becomes too full (exceeds its maximum capacity), it splits into two nodes. The middle (median) key is pushed up into the parent node. If the parent node is also full, this split cascades upwards. If the root node splits, the tree grows 1 level taller.
### 3. Deletion (Update/Remove)

* **Goal**: Remove a key without breaking the tree's structural rules.
* **How it works**: If the key is inside a leaf node, it is deleted directly. If the key is inside an internal node, it is swapped with its closest neighbor (the in-order predecessor or successor) to safely move the deletion down to a leaf node.
* **Handling Underflow**: If a node loses too many keys and drops below its minimum allowed capacity, it balances itself. It will first try to borrow a key from an adjacent sibling node. If the siblings are also running low on keys, it performs a merge, combining the underfilled node, its sibling, and a separating key from the parent into one node.
