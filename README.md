# DoubleClique

DoubleClique is a high‑performance, **in‑memory, replicated key–value store** that runs on Dolphin’s PCIe interconnect using the SISCI API.

## Quick tour

```
.
├── client/         # client process (benchmarks + GET/PUT implementations)
├── server/         # replica process (hash table + request region handlers)
├── common/         # shared utils, buddy allocator, protocol headers
├── cmake/          # FindSISCI.cmake helper
└── CMakeLists.txt  # top‑level build recipe
```

`REPLICA_COUNT` is fixed to **3**; change it in `common/index_data_protocol.h`.

---

## Building

> **Prerequisites**
>
> * C compiler with C17 support  
> * [SISCI headers](https://www.dolphinics.com/) installed in **/opt/DIS**  
> * CMake ≥ 3.28

```bash
git clone https://github.com/kritjo/DoubleClique.git
cd DoubleClique
cmake -B build
cmake --build build -j$(nproc)
```

This produces two binaries:

| Binary      | Location           | Purpose |
|-------------|--------------------|---------|
| `server`    | `build/server/`    | runs on each replica node |
| `client`    | `build/client/`    | benchmark / KV workload generator |

---

## Running a test cluster

1. **Start three replica nodes**

```bash
# on node 0
build/server/server 0
# on node 1
build/server/server 1
# on node 2
build/server/server 2
```

2. **Run the client** (pass the Dolphin *node IDs* of the replicas in **the order you prefer**):

```bash
# on the client host
build/client/client 20 24 28
```

---

## Acknowledgements
* **Paul Hsieh** – for the `SuperFastHash`.  
* **Stanislav Paskalev** – for the `buddy_alloc` implementation.
