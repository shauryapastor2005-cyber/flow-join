#pragma once
#include "phase3.h"

// Phase 1 -> Phase 2 -> 3a -> 3b -> local final invariants. Five exchanges, ten barriers.
inline void flow_join_worker(Server& s, const Workload& w, const Config& cfg) {
    phase1_exchange_R(s, w, cfg);
    phase2_exchange_S(s, w, cfg);
    phase3a_selective_broadcast(s);
    phase3b_sfr_replicate(s);
    require(s.held_r.empty(), "flow_join_worker: H_R not empty");
    require(s.held_s.empty(), "flow_join_worker: H_S not empty");
    require(s.m.empty(), "flow_join_worker: M not empty");
}

// Post-join and single-threaded: every server agrees with server 0 on lists and grids.
inline void check_flow_join_run(const ClusterRun& run) {
    require(!run.servers.empty(), "check_flow_join_run: no servers");
    const Server& s0 = run.servers[0];
    for (const Server& s : run.servers) {
        require(s.held_r.empty() && s.held_s.empty() && s.m.empty(), "check: H_R, H_S or M not empty");
        require(s.l_r == s0.l_r, "check: L_R differs from server 0");   // keys and counts
        require(s.l_s == s0.l_s, "check: L_S differs from server 0");
        require(s.counters.grids.size() == s0.counters.grids.size(), "check: grid count differs");
        for (size_t i = 0; i < s0.counters.grids.size(); ++i) {   // same grid per both-heavy key
            const GridInfo& a = s.counters.grids[i];
            const GridInfo& b = s0.counters.grids[i];
            require(a.key == b.key && a.rows == b.rows && a.cols == b.cols, "check: grid differs");
        }
    }
}