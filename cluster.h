#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common.h"
#include "workload.h"

enum class Kind { InsertH, InsertHB, InsertHF, ProbeH, ProbeHF };
const int kNumKinds = 5;
enum class Ship { Unicast, SBroadcast, SelBcastR, SfrR, SfrS };   // which counter
enum class Relation { R = 0, S = 1 };

struct KeyCount { uint64_t key = 0; uint64_t count = 0; };
struct Report { bool present = false; uint64_t n_s = 0; std::vector<KeyCount> keys; };
struct Install { bool present = false; std::vector<KeyCount> list; };
 
struct Batch { Kind kind = Kind::InsertH; std::vector<Tuple> tuples; };
struct Mailbox { std::mutex mu; std::vector<Batch> batches; };
struct Barrier {
    std::mutex mu; std::condition_variable cv;
    int n = 1; int waiting = 0; uint64_t generation = 0;
};
struct ControlBoard {
    std::mutex mu;
    std::vector<Report> reports;     // index = relation * n + server
    std::vector<Install> installs;   // same indexing
};
struct Cluster {
    int n;
    std::vector<Mailbox> mailboxes;
    Barrier barrier;
    ControlBoard board;
    explicit Cluster(int n_in) : n(n_in), mailboxes(n_in) {
        barrier.n = n_in;
        board.reports.resize(2 * n_in);
        board.installs.resize(2 * n_in);
    }
};

struct Detector {
    SpaceSaving hist;            // approximate key counts
    uint64_t n_s = 0;            // fragment size
    uint64_t d = 0;              // report at this tuple
    double threshold = 0.0;      // heavy-key cutoff
    uint64_t seen = 0;           // tuples observed
    bool report_posted = false; bool installed = false; bool consensus_done = false;
};

struct alignas(64) Server {   // one thread's private state
    int id = 0; int n = 1; size_t batch_size = 1024;
    Cluster* cluster = nullptr;
    Table h, h_b, h_f;                      // three hash tables
    std::vector<Tuple> held_r, held_s, m;   // parked tuples
    std::unordered_map<uint64_t, uint64_t> l_r, l_s;   // heavy key lists
    Detector det_r, det_s;
    std::vector<std::vector<Tuple>> out;    // index = dest * kNumKinds + kind
    Counters counters;
    JoinResult result;
};

// Probe table and emit 
inline void probe_table(Server& s, const Table& table, const Tuple& s_tuple) {
    const std::vector<uint64_t>* rows = table_probe(table, s_tuple.key, s.counters);
    if (rows == nullptr) return;   // no match
    for (uint64_t r_id : *rows) emit_pair(s.result, s.counters, r_id, s_tuple.id);
}

// Route tuple to its table
inline void apply_tuple(Server& s, Kind kind, const Tuple& t) {
    switch (kind) {
        case Kind::InsertH:  table_insert(s.h, t, s.counters); break;
        case Kind::InsertHB: table_insert(s.h_b, t, s.counters); break;
        case Kind::InsertHF: table_insert(s.h_f, t, s.counters); break;
        case Kind::ProbeH:   probe_table(s, s.h, t); break;     
        case Kind::ProbeHF:  probe_table(s, s.h_f, t); break;
    }
}

// Move one buffer to mailbox
inline void flush_buffer(Server& s, int dest, Kind kind) {
    std::vector<Tuple>& buf = s.out[dest * kNumKinds + static_cast<int>(kind)];
    if (buf.empty()) return;
    Batch b{kind, {}};
    b.tuples.swap(buf);
    Mailbox& box = s.cluster->mailboxes[dest];
    std::lock_guard<std::mutex> lock(box.mu);
    box.batches.push_back(std::move(b));
}

// Buffer a tuple for sending
inline void send(Server& s, int dest, Kind kind, Ship ship, const Tuple& t) {
    if (dest == s.id) { apply_tuple(s, kind, t); return; }   // to self: apply now
    Counters& c = s.counters;
    uint64_t* const slots[5] = {&c.shipped_unicast, &c.shipped_s_broadcast,
                                &c.shipped_sel_bcast_r, &c.shipped_sfr_r, &c.shipped_sfr_s};
    ++*slots[static_cast<int>(ship)];
    std::vector<Tuple>& buf = s.out[dest * kNumKinds + static_cast<int>(kind)];
    buf.push_back(t);
    if (buf.size() >= s.batch_size) flush_buffer(s, dest, kind);   // buffer full
}

// Send everything buffered
inline void flush(Server& s) {
    for (int dest = 0; dest < s.n; ++dest)
        for (int k = 0; k < kNumKinds; ++k) flush_buffer(s, dest, static_cast<Kind>(k));
}

// Everyone waits for everyone
inline void barrier_wait(Barrier& b) {
    std::unique_lock<std::mutex> lock(b.mu);
    uint64_t gen = b.generation;
    if (++b.waiting == b.n) { b.waiting = 0; ++b.generation; b.cv.notify_all(); return; }
    b.cv.wait(lock, [&b, gen] { return b.generation != gen; });
}

// Empty inbox, apply all
inline void drain(Server& s) {
    std::vector<Batch> taken;
    Mailbox& box = s.cluster->mailboxes[s.id];
    { std::lock_guard<std::mutex> lock(box.mu); taken.swap(box.batches); }   // grab all
    for (const Batch& b : taken)
        for (const Tuple& t : b.tuples) apply_tuple(s, b.kind, t);
}

// Send, sync, receive, sync
inline void exchange(Server& s) {
    flush(s);
    barrier_wait(s.cluster->barrier);   // all sends delivered
    drain(s);
    barrier_wait(s.cluster->barrier);   // all drains finished
}

inline size_t slot_of(Relation rel, int n, int server) {
    return static_cast<size_t>(rel) * static_cast<size_t>(n) + static_cast<size_t>(server);
}

// Write own report
inline void post_report(Server& s, Relation rel, uint64_t n_s,
                        const std::vector<KeyCount>& keys) {
    ControlBoard& b = s.cluster->board;
    std::lock_guard<std::mutex> lock(b.mu); //co-ordinator might be reading reports thus lock is imp
    Report& slot = b.reports[slot_of(rel, s.n, s.id)];
    slot.present = true; slot.n_s = n_s; slot.keys = keys;
}

// Server 0: read all reports
inline bool read_reports(Server& s, Relation rel, std::vector<Report>& out) {
    ControlBoard& b = s.cluster->board;
    std::lock_guard<std::mutex> lock(b.mu);
    size_t first = slot_of(rel, s.n, 0);
    for (int i = 0; i < s.n; ++i)
        if (!b.reports[first + i].present) return false;   // someone missing
    out.assign(b.reports.begin() + first, b.reports.begin() + first + s.n);
    return true;
}

// Server 0: send list to all
inline void post_installs(Server& s, Relation rel, const std::vector<KeyCount>& list) {
    require(s.id == 0, "post_installs: only server 0 may install");
    ControlBoard& b = s.cluster->board;
    std::lock_guard<std::mutex> lock(b.mu);
    size_t first = slot_of(rel, s.n, 0);
    for (int i = 0; i < s.n; ++i) {
        Install& slot = b.installs[first + i];
        slot.present = true; slot.list = list;
    }
}

// Read own install slot
inline bool read_install(Server& s, Relation rel, std::vector<KeyCount>& out) {
    ControlBoard& b = s.cluster->board;
    std::lock_guard<std::mutex> lock(b.mu);
    const Install& slot = b.installs[slot_of(rel, s.n, s.id)];
    if (!slot.present) return false;   // not arrived yet
    out = slot.list;
    return true;
}

using WorkerFn = void (*)(Server&, const Workload&, const Config&);
struct ClusterRun { JoinResult result; std::vector<Server> servers; };   // merged + final state

inline void server_main(Server* s, const Workload* w, const Config* cfg, WorkerFn worker) {
    uint64_t start = thread_cpu_ns();
    worker(*s, *w, *cfg);
    s->counters.cpu_ns = thread_cpu_ns() - start;   // CPU time used
}

inline void init_server(Server& s, int id, const Config& cfg, Cluster* cluster) {
    s.id = id; s.n = cfg.n; s.batch_size = cfg.batch_size; s.cluster = cluster;
    s.out.assign(static_cast<size_t>(cfg.n) * kNumKinds, std::vector<Tuple>());
    s.det_r.hist = make_space_saving(cfg.k); s.det_s.hist = make_space_saving(cfg.k);
}

// Run threads, merge results
inline ClusterRun run_cluster(const Workload& w, const Config& cfg, WorkerFn worker) {
    require(cfg.n >= 1 && w.n == cfg.n, "run_cluster: workload and config disagree on n");
    require(cfg.batch_size >= 1, "run_cluster: batch_size must be at least 1");
    Cluster cluster(cfg.n);
    ClusterRun run;
    run.servers.resize(cfg.n);
    for (int i = 0; i < cfg.n; ++i) init_server(run.servers[i], i, cfg, &cluster);
    std::vector<std::thread> threads;
    for (int i = 0; i < cfg.n; ++i)
        threads.emplace_back(server_main, &run.servers[i], &w, &cfg, worker);
    for (std::thread& t : threads) t.join();
    for (Server& s : run.servers) {
        for (const std::vector<Tuple>& buf : s.out) require(buf.empty(), "run_cluster: unflushed");
        require(cluster.mailboxes[s.id].batches.empty(), "run_cluster: undrained mailbox");
        run.result.count += s.result.count;          // wrapping sums
        run.result.checksum += s.result.checksum;
        s.cluster = nullptr;                         // cluster dies soon
    }
    return run;
}
