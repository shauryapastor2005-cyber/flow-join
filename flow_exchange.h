#pragma once
#include "consensus.h"

// ---------- Phase 1: exchange R ----------

// Before L_R is known (A4): a locally hot tuple is held, anything else goes to owner(k).
inline void route_r_before_install(Server& s, const Tuple& t, bool hot) {
    if (hot) {   // A4: hold in H_R until L_R is known
        s.held_r.push_back(t);
        ++s.counters.held_r;
        return;
    }
    send(s, owner(t.key, s.n), Kind::InsertH, Ship::Unicast, t);   // not hot: owner's H
}

// After L_R is known (A7): only membership in L_R decides, no local count.
inline void route_r_after_install(Server& s, const Tuple& t) {
    if (s.l_r.count(t.key) != 0) {   // A7: key in L_R, keep local
        table_insert(s.h, t, s.counters);
        return;
    }
    send(s, owner(t.key, s.n), Kind::InsertH, Ship::Unicast, t);   // A7: otherwise owner(k)
}

// A8: resolve means replay through the after-install rule, then H_R is empty.
inline void resolve_held_R(Server& s) {
    for (const Tuple& t : s.held_r) route_r_after_install(s, t);   // A8: same rule as later tuples
    s.held_r.clear();
}

// ---------- Phase 2: exchange S ----------

// Crucial Probe Rule: a broadcast copy probes H on every server (self included).
inline void broadcast_s(Server& s, const Tuple& t) {
    for (int dest = 0; dest < s.n; ++dest) send(s, dest, Kind::ProbeH, Ship::SBroadcast, t);
}

// Rule Set 1, before L_S is known.
inline void route_s_before_install(Server& s, const Tuple& t, bool hot) {
    if (hot) {   // A4: hold in H_S until L_S is known
        s.held_s.push_back(t);
        ++s.counters.held_s;
        return;
    }
    if (s.l_r.count(t.key) != 0) {   // Rule Set 1: key in L_R, broadcast
        broadcast_s(s, t);
        return;
    }
    send(s, owner(t.key, s.n), Kind::ProbeH, Ship::Unicast, t);   // Rule Set 1: owner(k)
}

// Rule Set 2, after L_S is known (A7): only the global lists decide.
inline void route_s_after_install(Server& s, const Tuple& t) {
    if (s.l_s.count(t.key) != 0) {   // Rule Set 2 / A7: key in L_S, materialize in M
        s.m.push_back(t);
        return;
    }
    if (s.l_r.count(t.key) != 0) {   // Rule Set 2: key in L_R minus L_S, broadcast
        broadcast_s(s, t);
        return;
    }
    send(s, owner(t.key, s.n), Kind::ProbeH, Ship::Unicast, t);   // Rule Set 2: owner(k)
}

// A8: resolve means replay through the after-install rule, then H_S is empty.
inline void resolve_held_S(Server& s) {
    for (const Tuple& t : s.held_s) route_s_after_install(s, t);   // A8: same rule as later tuples
    s.held_s.clear();
}

// ---------- scan loops ----------

// One R tuple: before install observe then route, after install route only.
inline void scan_tuple_R(Server& s, const Tuple& t, const Config& cfg) {
    if (!s.det_r.installed) {   // before L_R is known
        bool hot = detector_observe(s, Relation::R, t.key, cfg);
        route_r_before_install(s, t, hot);
    } else {                    // after L_R is known: no histogram (A7)
        route_r_after_install(s, t);
    }
}

inline void scan_tuple_S(Server& s, const Tuple& t, const Config& cfg) {
    if (!s.det_s.installed) {   // before L_S is known
        bool hot = detector_observe(s, Relation::S, t.key, cfg);
        route_s_before_install(s, t, hot);
    } else {                    // after L_S is known: no histogram (A7)
        route_s_after_install(s, t);
    }
}

inline void phase1_exchange_R(Server& s, const Workload& w, const Config& cfg) {
    require(cfg.morsel_size >= 1, "phase1_exchange_R: morsel_size must be at least 1");
    const std::vector<Tuple>& frag = w.frag_R[s.id];
    detector_begin(s, Relation::R, frag.size(), cfg);   // D_R, T_R (A3), fresh R histogram
    size_t pos = 0;
    while (pos < frag.size()) {
        size_t end = std::min(frag.size(), pos + cfg.morsel_size);
        for (; pos < end; ++pos) scan_tuple_R(s, frag[pos], cfg);
        if (detector_poll(s, Relation::R, cfg)) resolve_held_R(s);   // A8: install just arrived
    }
    if (detector_wait(s, Relation::R, cfg)) resolve_held_R(s);       // A8: install arrived at the end
    exchange(s);   // A9/A16: every R tuple is in exactly one H before Phase 2
}

inline void phase2_exchange_S(Server& s, const Workload& w, const Config& cfg) {
    require(cfg.morsel_size >= 1, "phase2_exchange_S: morsel_size must be at least 1");
    const std::vector<Tuple>& frag = w.frag_S[s.id];
    detector_begin(s, Relation::S, frag.size(), cfg);   // fresh S histogram, D_S, T_S
    size_t pos = 0;
    while (pos < frag.size()) {
        size_t end = std::min(frag.size(), pos + cfg.morsel_size);
        for (; pos < end; ++pos) scan_tuple_S(s, frag[pos], cfg);
        if (detector_poll(s, Relation::S, cfg)) resolve_held_S(s);   // A8: install just arrived
    }
    if (detector_wait(s, Relation::S, cfg)) resolve_held_S(s);       // A8: install arrived at the end
    exchange(s);   // Crucial Probe Rule: received S tuples probed H only
    s.counters.m_entering_phase3 = s.m.size();
    require(s.held_r.empty(), "phase2_exchange_S: H_R not empty (A4)");
    require(s.held_s.empty(), "phase2_exchange_S: H_S not empty (A4)");
    for (const Tuple& t : s.m)   // A7: only L_S keys may wait in M
        require(s.l_s.count(t.key) != 0, "phase2_exchange_S: M holds a key outside L_S (A7)");
}