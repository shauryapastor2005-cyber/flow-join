#pragma once
#include "flow_exchange.h"
#include <algorithm>
#include <utility>

// minimise (n1-1)*C_R + (n2-1)*C_S over n1*n2 = n; strict < keeps the smaller n1 on ties.
inline std::pair<int, int> choose_grid(int n, uint64_t c_r, uint64_t c_s) {
    require(n >= 1, "choose_grid: n must be at least 1");
    std::pair<int, int> best(1, n);
    uint64_t best_cost = static_cast<uint64_t>(n - 1) * c_s;   // n1 = 1
    for (int n1 = 2; n1 <= n; ++n1) {
        if (n % n1 != 0) continue;
        int n2 = n / n1;
        uint64_t cost = static_cast<uint64_t>(n1 - 1) * c_r + static_cast<uint64_t>(n2 - 1) * c_s;
        if (cost < best_cost) { best_cost = cost; best = {n1, n2}; }   
    }
    return best;
}

// Row-major grid s = u*n2 + v. R goes down a column: the n1 servers (u', v) sharing v
inline std::vector<int> column_of(int s, int n1, int n2) {
    require(n1 >= 1 && n2 >= 1 && s >= 0 && s < n1 * n2, "column_of: bad server or grid");
    std::vector<int> col;
    for (int u = 0; u < n1; ++u) col.push_back(u * n2 + s % n2);
    return col;
}

// S goes along a row: the n2 servers (u, v') sharing u
inline std::vector<int> row_of(int s, int n1, int n2) {
    require(n1 >= 1 && n2 >= 1 && s >= 0 && s < n1 * n2, "row_of: bad server or grid");
    std::vector<int> row;
    for (int v = 0; v < n2; ++v) row.push_back((s / n2) * n2 + v);
    return row;
}

// L_S keys in ascending order: those outside L_R (3a) or those also in L_R (3b).
inline std::vector<uint64_t> heavy_keys_sorted(const Server& s, bool also_in_l_r) {
    std::vector<uint64_t> keys;
    for (const auto& e : s.l_s)
        if ((s.l_r.count(e.first) != 0) == also_in_l_r) keys.push_back(e.first);
    std::sort(keys.begin(), keys.end());
    return keys;
}

//  Phase 3a: selective broadcast 

// owner(k) copies its H tuples of k to every other server's H_B and keeps the originals.
inline void broadcast_r_copies(Server& s, uint64_t key) {
    auto it = s.h.rows.find(key);
    if (it == s.h.rows.end()) return;
    for (uint64_t id : it->second)
        for (int dest = 0; dest < s.n; ++dest)
            if (dest != s.id) send(s, dest, Kind::InsertHB, Ship::SelBcastR, Tuple{key, id});
}

// selective probing of S only heavy keys
inline void probe_m_selective(Server& s) {
    std::vector<Tuple> rest;
    for (const Tuple& t : s.m) {
        if (s.l_r.count(t.key) != 0) { rest.push_back(t); continue; }   // stays in M for SFR
        const Table& build = (owner(t.key, s.n) == s.id) ? s.h : s.h_b;   // H at owner, else H_B
        probe_table(s, build, t);   // Phase 3a: leaves M
    }
    s.m.swap(rest);
}

inline void phase3a_selective_broadcast(Server& s) {
    for (uint64_t key : heavy_keys_sorted(s, false))
        if (owner(key, s.n) == s.id) broadcast_r_copies(s, key);   // Phase 3a: only owner(k) sends
    exchange(s);   // Phase 3a: every H_B copy is in place (always runs)
    probe_m_selective(s);
}

// Phase 3b: symmetric fragment replicate (keys in L_S and L_R)

using GridMap = std::unordered_map<uint64_t, std::pair<int, int>>;

// every server derives the grid of each both-heavy key from its own list copy, ascending.
inline GridMap derive_grids(Server& s, const std::vector<uint64_t>& keys) {
    GridMap grid;
    for (uint64_t key : keys) {
        std::pair<int, int> g = choose_grid(s.n, s.l_r.at(key), s.l_s.at(key));   
        grid[key] = g;
        s.counters.grids.push_back(GridInfo{key, g.first, g.second});
    }
    return grid;
}

// Phase 3b step 1: each local H tuple of k goes down its column into H_F (self included).
inline void replicate_r_along_columns(Server& s, const std::vector<uint64_t>& keys, const GridMap& grid) {
    for (uint64_t key : keys) {
        auto it = s.h.rows.find(key);
        if (it == s.h.rows.end()) continue;
        const std::pair<int, int>& g = grid.at(key);
        std::vector<int> column = column_of(s.id, g.first, g.second);   //  n1 servers sharing v
        for (uint64_t id : it->second)
            for (int dest : column) send(s, dest, Kind::InsertHF, Ship::SfrR, Tuple{key, id});   // H_F only
    }
}

// Phase 3b step 2: each remaining M tuple goes along its row and probes H_F only; M ends empty.
inline void replicate_s_along_rows(Server& s, const GridMap& grid) {
    for (const Tuple& t : s.m) {
        auto g = grid.find(t.key);
        require(g != grid.end(), "phase3b: M holds a key outside L_S and L_R");
        for (int dest : row_of(s.id, g->second.first, g->second.second))   // n2 servers sharing u
            send(s, dest, Kind::ProbeHF, Ship::SfrS, t);   // never probes H
    }
    s.m.clear();
}

inline void phase3b_sfr_replicate(Server& s) {
    std::vector<uint64_t> keys = heavy_keys_sorted(s, true);
    GridMap grid = derive_grids(s, keys);
    replicate_r_along_columns(s, keys, grid);   // Phase 3b step 1
    exchange(s);                                // Phase 3b: H_F complete 
    replicate_s_along_rows(s, grid);            // Phase 3b step 2
    exchange(s);                                // Phase 3b: every row copy probed H_F 
    require(s.m.empty(), "phase3b_sfr_replicate: M not empty after step 2");
}