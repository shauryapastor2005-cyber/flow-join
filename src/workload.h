#pragma once
#include <algorithm>
#include <array>
#include <cmath>
#include <vector>
#include "common.h"

struct Workload {
    int n = 0;                                       // number of fragments
    std::vector<std::vector<Tuple>> frag_R;          // n contiguous fragments of R
    std::vector<std::vector<Tuple>> frag_S;          // n contiguous fragments of S
    std::unordered_map<uint64_t, uint64_t> freq_R;   // key -> frequency in R
    std::unordered_map<uint64_t, uint64_t> freq_S;   // key -> frequency in S
    uint64_t expected_output = 0;                    // sum over k of freq_R(k) * freq_S(k)
    std::array<uint64_t, 3> heavy{};                 // heavy keys a, b, c
    std::vector<uint64_t> expected_L_R;              // expected global heavy set in R
    std::vector<uint64_t> expected_L_S;              // expected global heavy set in S
};

// a, b, c: first three integers >= 1 with pairwise distinct owners 
inline std::array<uint64_t, 3> pick_heavy_keys(int n) {
    if (n < 3) return {1, 2, 3};
    std::array<uint64_t, 3> h{};
    int found = 0;
    for (uint64_t key = 1; found < 3; ++key) {
        bool clash = false;
        for (int i = 0; i < found; ++i) clash = clash || owner(h[i], n) == owner(key, n);
        if (!clash) h[found++] = key;
    }
    return h;
}

inline uint64_t scaled(uint64_t base, double f) {
    uint64_t v = static_cast<uint64_t>(std::llround(static_cast<double>(base) * f));
    return v < 1 ? 1 : v;
}

//  Step 1: plant exact counts for heavy keys
inline void plant(std::vector<uint64_t>& keys, uint64_t key, uint64_t count) {
    keys.insert(keys.end(), static_cast<size_t>(count), key);
}

// Step 2: fill the rest 
inline std::vector<uint64_t> light_keys(uint64_t count, const std::array<uint64_t, 3>& h) {
    std::vector<uint64_t> pool;  // the first `count` integers >= 1 that are not a, b or c
    for (uint64_t key = 1; pool.size() < count; ++key)
        if (key != h[0] && key != h[1] && key != h[2]) pool.push_back(key);
    return pool;
}

inline void fill_distinct(std::vector<uint64_t>& keys, const std::vector<uint64_t>& pool) {
    keys.insert(keys.end(), pool.begin(), pool.end());  // each key exactly once
}

inline void fill_uniform(std::vector<uint64_t>& keys, uint64_t count,
                         const std::vector<uint64_t>& pool, Rng& rng) {
    for (uint64_t i = 0; i < count; ++i) keys.push_back(pool[rng_below(rng, pool.size())]);
}

//  Step 3: shuffle with the seed, then cut into n contiguous fragments 
inline void shuffle_keys(std::vector<uint64_t>& keys, Rng& rng) {  // Fisher-Yates
    for (size_t i = keys.size(); i > 1; --i) std::swap(keys[i - 1], keys[rng_below(rng, i)]);
}

inline std::vector<std::vector<Tuple>> cut_fragments(const std::vector<uint64_t>& keys, int n) {
    std::vector<std::vector<Tuple>> frags(static_cast<size_t>(n));
    size_t base = keys.size() / static_cast<size_t>(n), extra = keys.size() % static_cast<size_t>(n);
    size_t pos = 0;
    for (size_t f = 0; f < frags.size(); ++f) {
        size_t len = base + (f < extra ? 1 : 0);  
        for (size_t i = 0; i < len; ++i, ++pos) frags[f].push_back(Tuple{keys[pos], pos});
    }
    return frags;
}

inline std::unordered_map<uint64_t, uint64_t> count_frequencies(const std::vector<uint64_t>& keys) {
    std::unordered_map<uint64_t, uint64_t> freq;
    for (uint64_t k : keys) freq[k] += 1;
    return freq;
}

inline uint64_t expected_output_of(const Workload& w) {
    uint64_t total = 0;
    for (const auto& kv : w.freq_R) {
        auto it = w.freq_S.find(kv.first);
        if (it != w.freq_S.end()) total += kv.second * it->second;
    }
    return total;
}

// Shuffle both relations, record frequencies and expected output, cut into fragments.
inline Workload assemble(const Config& cfg, std::vector<uint64_t> keys_r,
                         std::vector<uint64_t> keys_s, Rng& rng) {
    Workload w;
    w.n = cfg.n;
    w.heavy = pick_heavy_keys(cfg.n);
    shuffle_keys(keys_r, rng);
    shuffle_keys(keys_s, rng);
    w.freq_R = count_frequencies(keys_r);
    w.freq_S = count_frequencies(keys_s);
    w.expected_output = expected_output_of(w);
    w.frag_R = cut_fragments(keys_r, cfg.n);
    w.frag_S = cut_fragments(keys_s, cfg.n);
    return w;
}

// W1 uniform: R has all-distinct keys, S keys uniform over R's keys. L_R and L_S are empty.
inline Workload make_w1(const Config& cfg, double scale) {
    Rng rng = rng_seed(cfg.seed);
    std::vector<uint64_t> pool, kr, ks;
    for (uint64_t key = 1; key <= scaled(200000, scale); ++key) pool.push_back(key);
    fill_distinct(kr, pool);                              // fill R (nothing to plant)
    fill_uniform(ks, scaled(2000000, scale), pool, rng);  // fill S
    return assemble(cfg, kr, ks, rng);
}

// W2 S-only skew: a, b, c x5 in R; S has a 20%, b 10%, c 5%, the rest uniform over light keys.
inline Workload make_w2(const Config& cfg, double scale) {
    Rng rng = rng_seed(cfg.seed);
    std::array<uint64_t, 3> h = pick_heavy_keys(cfg.n);
    uint64_t nr = scaled(200000, scale), ns = scaled(2000000, scale);
    require(nr > 15, "W2 scale too small");
    std::vector<uint64_t> pool = light_keys(nr - 15, h), kr, ks;
    for (uint64_t key : h) plant(kr, key, 5);  // plant R
    fill_distinct(kr, pool);                   // fill R
    uint64_t ca = ns / 5, cb = ns / 10, cc = ns / 20;
    plant(ks, h[0], ca);                       // plant S
    plant(ks, h[1], cb);
    plant(ks, h[2], cc);
    fill_uniform(ks, ns - ca - cb - cc, pool, rng);  // fill S
    Workload w = assemble(cfg, kr, ks, rng);
    w.expected_L_S = {h[0], h[1], h[2]};
    return w;
}

// W3 R and S skew. size_factor = 1 is full size (Section 3.2); 0.01 is the reduced sanitizer size.
inline Workload make_w3(const Config& cfg, double size_factor) {
    Rng rng = rng_seed(cfg.seed);
    std::array<uint64_t, 3> h = pick_heavy_keys(cfg.n);
    uint64_t ra = scaled(8000, size_factor), rb = scaled(4000, size_factor), rc = scaled(3000, size_factor);
    uint64_t sa = scaled(12000, size_factor), sb = scaled(10000, size_factor);
    uint64_t r_other = scaled(85000, size_factor), s_other = scaled(378000, size_factor);
    std::vector<uint64_t> light = light_keys(r_other, h), kr, ks;
    plant(kr, h[0], ra);                       // plant R
    plant(kr, h[1], rb);
    plant(kr, h[2], rc);
    fill_distinct(kr, light);                  // fill R
    plant(ks, h[0], sa);                       // plant S
    plant(ks, h[1], sb);
    std::vector<uint64_t> s_pool = light;      // every R key except a and b: c plus the light keys
    s_pool.push_back(h[2]);
    fill_uniform(ks, s_other, s_pool, rng);    // fill S
    Workload w = assemble(cfg, kr, ks, rng);
    w.expected_L_R = {h[0], h[1], h[2]};
    w.expected_L_S = {h[0], h[1]};
    return w;
}

// ---- Partition-correlated (tuple-placement) skew: same key multiset, different placement ----
// A fraction rho of heavy key h[j]'s tuples goes to home fragment j % n, the rest to uniformly random
// fragments (a full fragment spills to the next). Light tuples fill fragments to the cut_fragments
// sizes; each fragment is shuffled internally. Frequencies and expected output are unchanged.
inline std::vector<std::vector<Tuple>> place_clustered(const std::vector<std::vector<Tuple>>& frags_in,
                                                       const std::array<uint64_t, 3>& h, double rho,
                                                       Rng& rng) {
    int n = static_cast<int>(frags_in.size());
    std::vector<uint64_t> heavy, light;
    std::vector<size_t> cap;
    for (const auto& f : frags_in) {
        cap.push_back(f.size());
        for (const Tuple& t : f) (t.key == h[0] || t.key == h[1] || t.key == h[2] ? heavy : light).push_back(t.key);
    }
    std::sort(heavy.begin(), heavy.end());   // deterministic order before the random placement
    std::vector<std::vector<uint64_t>> keys(static_cast<size_t>(n));
    for (uint64_t k : heavy) {
        int home = k == h[0] ? 0 : (k == h[1] ? 1 : 2);
        double u = double(rng_next(rng) >> 11) * (1.0 / 9007199254740992.0);
        int f = (u < rho) ? home % n : static_cast<int>(rng_below(rng, static_cast<uint64_t>(n)));
        while (keys[f].size() >= cap[f]) f = (f + 1) % n;   // spill if full
        keys[f].push_back(k);
    }
    shuffle_keys(light, rng);
    size_t li = 0;
    for (int f = 0; f < n; ++f) {
        while (keys[f].size() < cap[f]) keys[f].push_back(light[li++]);
        shuffle_keys(keys[f], rng);
    }
    std::vector<std::vector<Tuple>> out(static_cast<size_t>(n));
    uint64_t pos = 0;
    for (int f = 0; f < n; ++f)
        for (uint64_t k : keys[f]) out[f].push_back(Tuple{k, pos++});
    return out;
}

inline Workload make_clustered(Workload w, const Config& cfg, double rho) {
    Rng rng = rng_seed(cfg.seed ^ 0xC1A57E2ULL);
    w.frag_R = place_clustered(w.frag_R, w.heavy, rho, rng);
    w.frag_S = place_clustered(w.frag_S, w.heavy, rho, rng);
    return w;
}

// W4: W3's keys (two-sided skew, SFR path) with clustered placement.
inline Workload make_w4(const Config& cfg, double size_factor, double rho) {
    return make_clustered(make_w3(cfg, size_factor), cfg, rho);
}

// W5: W2's keys (S-only skew, selective-broadcast path) with clustered placement.
inline Workload make_w5(const Config& cfg, double scale, double rho) {
    return make_clustered(make_w2(cfg, scale), cfg, rho);
}

// Single-threaded hash join over all of R and S
inline JoinResult reference_join(const Workload& w, Counters& c) {
    Table h;
    for (const auto& frag : w.frag_R)
        for (const Tuple& t : frag) table_insert(h, t, c);
    JoinResult out;
    for (const auto& frag : w.frag_S)
        for (const Tuple& s : frag) {
            const std::vector<uint64_t>* ids = table_probe(h, s.key, c);
            if (ids == nullptr) continue;
            for (uint64_t r_id : *ids) emit_pair(out, c, r_id, s.id);
        }
    return out;
}
