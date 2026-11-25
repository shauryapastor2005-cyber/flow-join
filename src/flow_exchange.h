#pragma once
#include "consensus.h"

// Phase 1: exchange R 

// Before L_R is known, a locally hot tuple is held, anything else goes to owner(k).
inline void route_r_before_install(Server& s, const Tuple& t, bool hot) {
    if (hot) {   //locally heavy
        s.held_r.push_back(t);
        ++s.counters.held_r;
        return;
    }
    send(s, owner(t.key, s.n), Kind::InsertH, Ship::Unicast, t);   //otherwise owner(k)
}

// After L_R is known, only membership in L_R decides, no local count.
inline void route_r_after_install(Server& s, const Tuple& t) {
    if (s.l_r.count(t.key) != 0) {   //heavy key
        int d = qpsr_dest(s, Relation::R, t.key);   // QPSR: surplus above quota leaves
        if (d >= 0) { send(s, d, Kind::InsertH, Ship::Rebalance, t); return; }
        table_insert(s.h, t, s.counters);
        return;
    }
    send(s, owner(t.key, s.n), Kind::InsertH, Ship::Unicast, t);   //otherwise owner(k)
}

inline void resolve_held_R(Server& s) {
    for (const Tuple& t : s.held_r) route_r_after_install(s, t);   
    s.held_r.clear();
}

//  Phase 2: exchange S 

// a broadcast copy probes H on every server (self included)
inline void broadcast_s(Server& s, const Tuple& t) {
    for (int dest = 0; dest < s.n; ++dest) send(s, dest, Kind::ProbeH, Ship::SBroadcast, t);
}

// before L_S is known
inline void route_s_before_install(Server& s, const Tuple& t, bool hot) {
    if (hot) {   //  hold in H_S until L_S is known
        s.held_s.push_back(t);
        ++s.counters.held_s;
        return;
    }
    if (s.l_r.count(t.key) != 0) {   // key in L_R, broadcast
        broadcast_s(s, t);
        return;
    }
    send(s, owner(t.key, s.n), Kind::ProbeH, Ship::Unicast, t);   //  owner(k)
}

// after L_S is known 
inline void route_s_after_install(Server& s, const Tuple& t) {
    if (s.l_s.count(t.key) != 0) {   //  materialize in M
        int d = qpsr_dest(s, Relation::S, t.key);   // QPSR: surplus above quota goes to a deficit M
        if (d >= 0) { send(s, d, Kind::ToM, Ship::Rebalance, t); return; }
        s.m.push_back(t);
        return;
    }
    if (s.l_r.count(t.key) != 0) {   // broadcast
        broadcast_s(s, t);
        return;
    }
    send(s, owner(t.key, s.n), Kind::ProbeH, Ship::Unicast, t);   // owner(k)
}

inline void resolve_held_S(Server& s) {
    for (const Tuple& t : s.held_s) route_s_after_install(s, t);
    s.held_s.clear();
}

// scan loops 

inline void scan_tuple_R(Server& s, const Tuple& t, const Config& cfg) {
    if (!s.det_r.installed) {   // before L_R is known
        bool hot = detector_observe(s, Relation::R, t.key, cfg);
        route_r_before_install(s, t, hot);
    } else {                    // after L_R is known
        route_r_after_install(s, t);
    }
}

inline void scan_tuple_S(Server& s, const Tuple& t, const Config& cfg) {
    if (!s.det_s.installed) {   // before L_S is known
        bool hot = detector_observe(s, Relation::S, t.key, cfg);
        route_s_before_install(s, t, hot);
    } else {                    // after L_S is known
        route_s_after_install(s, t);
    }
}

inline void phase1_exchange_R(Server& s, const Workload& w, const Config& cfg) {
    require(cfg.morsel_size >= 1, "phase1_exchange_R: morsel_size must be at least 1");
    const std::vector<Tuple>& frag = w.frag_R[s.id];
    detector_begin(s, Relation::R, frag.size(), cfg);   //setup the histogram an space saving
    size_t pos = 0;
    while (pos < frag.size()) {
        size_t end = std::min(frag.size(), pos + cfg.morsel_size);
        for (; pos < end; ++pos) scan_tuple_R(s, frag[pos], cfg);
        if (detector_poll(s, Relation::R, cfg)) resolve_held_R(s);   
    }
    if (detector_wait(s, Relation::R, cfg)) resolve_held_R(s);       
    exchange(s);   //flush the remaining buffer, then barrier , then drain the mailboxes ,then barrier
}

inline void phase2_exchange_S(Server& s, const Workload& w, const Config& cfg) {
    require(cfg.morsel_size >= 1, "phase2_exchange_S: morsel_size must be at least 1");
    const std::vector<Tuple>& frag = w.frag_S[s.id];
    detector_begin(s, Relation::S, frag.size(), cfg);   //setup the histogram an space saving
    size_t pos = 0;
    while (pos < frag.size()) {
        size_t end = std::min(frag.size(), pos + cfg.morsel_size);
        for (; pos < end; ++pos) scan_tuple_S(s, frag[pos], cfg);
        if (detector_poll(s, Relation::S, cfg)) resolve_held_S(s);  
    }
    if (detector_wait(s, Relation::S, cfg)) resolve_held_S(s);     
    exchange(s);   //flush the remaining buffer, then barrier , then drain the mailboxes ,then barrier
    s.counters.m_entering_phase3 = s.m.size();
    
    //checks before proceeding to phase3
    require(s.held_r.empty(), "phase2_exchange_S: H_R not empty ");
    require(s.held_s.empty(), "phase2_exchange_S: H_S not empty ");
    for (const Tuple& t : s.m)
        require(s.l_s.count(t.key) != 0, "phase2_exchange_S: M holds a key outside L_S ");
}