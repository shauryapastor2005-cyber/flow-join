#pragma once
#include "cluster.h"
#include <algorithm>
#include <cmath>
#include <thread>

inline void sort_by_key(std::vector<KeyCount>& list) {
    std::sort(list.begin(), list.end(),
              [](const KeyCount& a, const KeyCount& b) { return a.key < b.key; });
}

// D = max(1, ceil(p * |X_s|))
inline uint64_t detection_budget(uint64_t fragment_size, double p) {
    uint64_t d = static_cast<uint64_t>(std::ceil(p * double(fragment_size)));
    return std::max<uint64_t>(1, d);
}

// Server-level candidate filtering 
inline std::vector<KeyCount> build_report(const SpaceSaving& hist, uint64_t n_s, double theta) {
    std::vector<KeyCount> keys;
    for (const SSEntry& e : hist.entries) {
        if (double(e.count) >= theta * double(n_s)) {   // locally heavy
            keys.push_back({e.key, e.count});
        }
    }
    sort_by_key(keys);
    return keys;
}

// Coordinator aggregation
inline std::vector<KeyCount> merge_reports(const std::vector<Report>& reports, double theta) {
    uint64_t n_total = 0;
    std::unordered_map<uint64_t, uint64_t> sums;
    for (const Report& r : reports) {
        n_total += r.n_s;
        for (const KeyCount& kc : r.keys) {
            sums[kc.key] += kc.count;
        }
    }
    std::vector<KeyCount> list;
    if (n_total == 0) return list;
    for (const auto& entry : sums) {
        if (double(entry.second) >= theta * double(n_total)) {   //  globally heavy
            list.push_back({entry.first, entry.second});
        }
    }
    sort_by_key(list);
    return list;
}

inline Detector& detector_of(Server& s, Relation rel) {
    return rel == Relation::R ? s.det_r : s.det_s;
}

inline std::unordered_map<uint64_t, uint64_t>& list_of(Server& s, Relation rel) {
    return rel == Relation::R ? s.l_r : s.l_s;
}

inline void detector_begin(Server& s, Relation rel, uint64_t fragment_size, const Config& cfg) {
    Detector& det = detector_of(s, rel);
    det.hist = make_space_saving(cfg.k);
    det.d = detection_budget(fragment_size, cfg.p);
    det.threshold = cfg.theta * double(det.d);   
    det.seen = 0;
    det.report_posted = false;
    det.installed = false;
    det.consensus_done = false;
    if (fragment_size == 0) {   
        post_report(s, rel, 0, {});
        det.report_posted = true;
    }
}

// snapshot the histogram, post the report.
inline void detector_trigger(Server& s, Relation rel, const Config& cfg) {
    Detector& det = detector_of(s, rel);
    post_report(s, rel, det.d, build_report(det.hist, det.d, cfg.theta));   // triggered at n_s=Dth tuple
    det.report_posted = true;
}

// Returns true if "locally hot"   
inline bool detector_observe(Server& s, Relation rel, uint64_t key, const Config& cfg) {
    Detector& det = detector_of(s, rel);
    if (det.installed) return false;   
    ss_update(det.hist, key);
    ++det.seen;
    bool hot = double(ss_count(det.hist, key)) >= det.threshold; 
    if (det.seen == det.d && !det.report_posted) {   // trigger exactly at the D-th tuple
        detector_trigger(s, rel, cfg);
    }
    return hot;
}

// coordinator duty on server 0, then every server reads its own install
inline bool detector_poll(Server& s, Relation rel, const Config& cfg) {
    Detector& det = detector_of(s, rel);
    std::vector<Report> reports;
    if (s.id == 0 && !det.consensus_done && read_reports(s, rel, reports)) {   // all n reports in
        post_installs(s, rel, merge_reports(reports, cfg.theta));   
        det.consensus_done = true;
    }
    std::vector<KeyCount> install;
    if (det.installed || !read_install(s, rel, install)) return false; // already installed or install not arrived then skip
    std::unordered_map<uint64_t, uint64_t>& heavy = list_of(s, rel);   
    heavy.clear();
    for (const KeyCount& kc : install) {
        heavy[kc.key] = kc.count;
    }
    det.installed = true; //installed successfully.   
    if (rel == Relation::R) {
        s.counters.l_r = install.size();
        s.counters.scanned_r_before_install = det.seen;
    } else {
        s.counters.l_s = install.size();
        s.counters.scanned_s_before_install = det.seen;
    }
    return true;
}

// check for installs if they have not arrived .
inline bool detector_wait(Server& s, Relation rel, const Config& cfg) {
    Detector& det = detector_of(s, rel);
    bool arrived = false;
    while (!det.installed) {
        arrived = detector_poll(s, rel, cfg);
        if (!arrived) std::this_thread::yield();
    }
    return arrived;
}
