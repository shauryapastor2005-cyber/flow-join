#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <unordered_map>
#include <vector>


struct Tuple {
    uint64_t key;
    uint64_t id;  
};

struct Config {
    int n = 4;                 // servers (threads)
    int k = 128;               // Space-Saving capacity
    double theta = 0.01;       // heavy threshold
    double p = 0.01;           // detection budget fraction
    size_t morsel_size = 4096; // tuples per scan morsel
    size_t batch_size = 1024;  // tuples per network batch
    uint64_t seed = 1;
    // QPSR extension (off = original Flow-Join)
    bool qpsr = false;         // Quota-Planned Surplus Routing of heavy tuples
    double qpsr_z = 2.0;       // significance gate: move only if surplus > z*sqrt(prefix count)
};

struct JoinResult {
    uint64_t count = 0;     // number of output pairs
    uint64_t checksum = 0;  // wrapping sum of pair_checksum over all pairs (order independent)
};


inline void require(bool cond, const char* msg) {
    if (!cond) {
        std::fprintf(stderr, "require failed: %s\n", msg);
        std::abort();
    }
}

// Hashing 
const uint64_t kHashSeed = 0x2545F4914F6CDD1DULL;

inline uint64_t mix64(uint64_t z) {  // splitmix64 finalizer
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

inline uint64_t hash64(uint64_t key) { return mix64(key + kHashSeed); }

// owner(key) = h(key) mod n
inline int owner(uint64_t key, int n) { return static_cast<int>(hash64(key) % static_cast<uint64_t>(n)); }

// Rng  
struct Rng {
    uint64_t state;
};

inline Rng rng_seed(uint64_t seed) { return Rng{seed}; }

inline uint64_t rng_next(Rng& r) {
    r.state += 0x9E3779B97F4A7C15ULL;
    return mix64(r.state);
}

inline uint64_t rng_below(Rng& r, uint64_t bound) { return rng_next(r) % bound; }  // bound > 0

// Counters; each server owns one 
struct GridInfo {
    uint64_t key;  // a both-heavy key
    int rows;
    int cols;
};

struct Counters {
    uint64_t work_units = 0;  // 1 per table insert, 1 per probe call, 1 per output pair
    uint64_t cpu_ns = 0;      // per-thread CPU time, end - start
    uint64_t shipped_unicast = 0;
    uint64_t shipped_s_broadcast = 0;
    uint64_t shipped_sel_bcast_r = 0;  // selective-broadcast R copies
    uint64_t shipped_sfr_r = 0;        // SFR R copies
    uint64_t shipped_sfr_s = 0;        // SFR S copies
    uint64_t shipped_rebalance = 0;    // QPSR surplus moves (0 in original Flow-Join)
    // Flow-Join diagnostics
    uint64_t l_r = 0;                      // |L_R|
    uint64_t l_s = 0;                      // |L_S|
    uint64_t held_r = 0;                   // R tuples held before install
    uint64_t held_s = 0;                   // S tuples held before install
    uint64_t scanned_r_before_install = 0; // R tuples scanned before L_R install
    uint64_t scanned_s_before_install = 0; // S tuples scanned before L_S install
    uint64_t m_entering_phase3 = 0;        // |M| entering Phase 3
    std::vector<GridInfo> grids;           // one entry per both-heavy key
};

inline uint64_t shipped_total(const Counters& c) {
    return c.shipped_unicast + c.shipped_s_broadcast + c.shipped_sel_bcast_r + c.shipped_sfr_r +
           c.shipped_sfr_s + c.shipped_rebalance;
}

// Timers
inline double wall_seconds() {
    using clock = std::chrono::steady_clock;
    return std::chrono::duration<double>(clock::now().time_since_epoch()).count();
}

// CPU time consumed by the calling thread 
inline uint64_t thread_cpu_ns() {
    timespec ts;
    require(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0, "clock_gettime failed");
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}

//  Space-Saving 
struct SSEntry {
    uint64_t key;
    uint64_t count;
};

struct SpaceSaving {
    size_t k = 128;
    std::vector<SSEntry> entries;                 // at most k counters
    std::unordered_map<uint64_t, size_t> slot;    // key -> index into entries
};

inline SpaceSaving make_space_saving(int k) {
    SpaceSaving s;
    s.k = static_cast<size_t>(k);
    return s;
}

// Index of the minimum counter
inline size_t ss_min_index(const SpaceSaving& s) {
    size_t best = 0;
    for (size_t i = 1; i < s.entries.size(); ++i) {
        const SSEntry& e = s.entries[i];
        const SSEntry& b = s.entries[best];
        if (e.count < b.count || (e.count == b.count && e.key < b.key)) best = i;
    }
    return best;
}

inline void ss_update(SpaceSaving& s, uint64_t key) {
    auto it = s.slot.find(key);
    if (it != s.slot.end()) {  
        s.entries[it->second].count += 1;
    } else if (s.entries.size() < s.k) {  
        s.slot[key] = s.entries.size();
        s.entries.push_back(SSEntry{key, 1});
    } else {  
        size_t i = ss_min_index(s);
        s.slot.erase(s.entries[i].key);
        s.entries[i] = SSEntry{key, s.entries[i].count + 1};
        s.slot[key] = i;
    }
}

inline uint64_t ss_count(const SpaceSaving& s, uint64_t key) {  // 0 if unlisted
    auto it = s.slot.find(key);
    return it == s.slot.end() ? 0 : s.entries[it->second].count;
}

inline uint64_t ss_sum(const SpaceSaving& s) {
    uint64_t total = 0;
    for (const SSEntry& e : s.entries) total += e.count;
    return total;
}

//  Table: key -> list of tuple ids (H, H_B and H_F) 
struct Table {
    std::unordered_map<uint64_t, std::vector<uint64_t>> rows;
};

inline void table_insert(Table& t, const Tuple& tup, Counters& c) {
    t.rows[tup.key].push_back(tup.id);
    c.work_units += 1;  // 1 per table insert
}

inline const std::vector<uint64_t>* table_probe(const Table& t, uint64_t key, Counters& c) {
    c.work_units += 1;  // 1 per probe call
    auto it = t.rows.find(key);
    return it == t.rows.end() ? nullptr : &it->second;
}

// Output pairs 
inline uint64_t pair_checksum(uint64_t r_id, uint64_t s_id) { return hash64(hash64(r_id) + s_id); }

inline void emit_pair(JoinResult& out, Counters& c, uint64_t r_id, uint64_t s_id) {
    out.count += 1;
    out.checksum += pair_checksum(r_id, s_id);  // wrapping sum: order independent
    c.work_units += 1;                          // 1 per output pair
}
