#pragma once

#include "cluster.h"

// Send each tuple to its owner
inline void send_fragment_to_owner(Server& s, const std::vector<Tuple>& frag, Kind kind) {
    for (const Tuple& t : frag) send(s, owner(t.key, s.n), kind, Ship::Unicast, t);
}

// Plain distributed hash join
inline void conventional_worker(Server& s, const Workload& w, const Config&) {
    send_fragment_to_owner(s, w.frag_R[s.id], Kind::InsertH);   // build side
    exchange(s);
    send_fragment_to_owner(s, w.frag_S[s.id], Kind::ProbeH);    // probe side
    exchange(s);
}
