// benchmark.cpp - Runs reference, conventional, and Flow-Join benchmarks,
// verifies correctness, and writes timing and balance metrics to CSV files.

#include "conventional.h"
#include "flow_join.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

const std::vector<std::string> ALL_WORKLOADS = {"w1", "w2", "w3"};
const std::vector<std::string> ALL_ALGOS     = {"ref", "conv", "flow"};

struct Options {
    std::vector<std::string> workloads;
    std::vector<std::string> algos;
    std::vector<int> server_counts = {4};
    int reps = 5;
    double scale = 1.0;
    bool scale_set = false;
    bool reduced = false;
};

struct RunRecord {
    double time_ms = 0.0;
    bool correct = false;
    JoinResult result;
    std::vector<Counters> servers;
    std::vector<uint64_t> heavy_r;  // sorted L_R keys from server 0 (flow only)
    std::vector<uint64_t> heavy_s;  // sorted L_S keys from server 0 (flow only)
};

struct RunStats {
    uint64_t work_max = 0;
    uint64_t shipped = 0;
    double work_mean = 0.0;
    double work_imbalance = 1.0;
    double cpu_imbalance = 1.0;
};

static void print_usage_and_exit() {
    std::fprintf(stderr,
        "usage: flowjoin (--all | --workload w1|w2|w3|all --algo ref|conv|flow|all) "
        "[--n N[,N...]] [--reps R] [--scale F | --reduced]\n");
    std::exit(2);
}

static bool contains(const std::vector<std::string>& list, const std::string& item) {
    return std::find(list.begin(), list.end(), item) != list.end();
}

static std::vector<int> parse_int_list(const std::string& text) {
    if (text.empty() || text.back() == ',') print_usage_and_exit();

    std::vector<int> values;
    std::stringstream ss(text);
    std::string token;

    while (std::getline(ss, token, ',')) {
        try {
            size_t pos = 0;
            int val = std::stoi(token, &pos);
            if (pos != token.size() || val < 1 || val > 100000) print_usage_and_exit();
            values.push_back(val);
        } catch (...) {
            print_usage_and_exit();
        }
    }
    return values;
}

static std::vector<std::string> parse_choice(const std::string& val,
                                             const std::vector<std::string>& allowed) {
    if (val == "all") return allowed;
    if (!contains(allowed, val)) print_usage_and_exit();
    return {val};
}

static Options parse_args(int argc, char** argv) {
    Options opt;
    bool run_all = false;

    for (int i = 1; i < argc; i++) {
        std::string flag = argv[i];
        if (flag == "--all") {
            run_all = true;
            continue;
        }
        if (flag == "--reduced") {
            opt.reduced = true;
            continue;
        }
        if (i + 1 >= argc) print_usage_and_exit();
        std::string val = argv[++i];

        if (flag == "--workload") {
            opt.workloads = parse_choice(val, ALL_WORKLOADS);
        } else if (flag == "--algo") {
            opt.algos = parse_choice(val, ALL_ALGOS);
        } else if (flag == "--n") {
            opt.server_counts = parse_int_list(val);
        } else if (flag == "--reps") {
            std::vector<int> parsed = parse_int_list(val);
            if (parsed.size() != 1) print_usage_and_exit();
            opt.reps = parsed[0];
        } else if (flag == "--scale") {
            try {
                size_t pos = 0;
                opt.scale = std::stod(val, &pos);
                opt.scale_set = true;
                if (pos != val.size() || opt.scale <= 0.0) print_usage_and_exit();
            } catch (...) {
                print_usage_and_exit();
            }
        } else {
            print_usage_and_exit();
        }
    }

    if (run_all) {
        opt.workloads = ALL_WORKLOADS;
        opt.algos = ALL_ALGOS;
    }
    if (opt.workloads.empty() || opt.algos.empty() || (opt.reduced && opt.scale_set)) {
        print_usage_and_exit();
    }
    return opt;
}

static Workload build_workload(const std::string& name, const Config& cfg, const Options& opt) {
    double scale = opt.reduced ? 0.01 : opt.scale;
    if (name == "w1") return make_w1(cfg, scale);
    if (name == "w2") return make_w2(cfg, scale);
    return make_w3(cfg, scale);
}

static std::vector<uint64_t> extract_sorted_keys(const std::unordered_map<uint64_t, uint64_t>& map) {
    std::vector<uint64_t> keys;
    keys.reserve(map.size());
    for (const auto& entry : map) keys.push_back(entry.first);
    std::sort(keys.begin(), keys.end());
    return keys;
}

// Executes one timed repetition of ref, conv, or flow.
static RunRecord run_once(const std::string& algo, const Workload& w, const Config& cfg) {
    RunRecord rec;

    if (algo == "ref") {
        Counters c;
        double start_wall = wall_seconds();
        uint64_t start_cpu = thread_cpu_ns();
        rec.result = reference_join(w, c);
        c.cpu_ns = thread_cpu_ns() - start_cpu;
        rec.time_ms = (wall_seconds() - start_wall) * 1000.0;
        rec.servers.push_back(c);
        return rec;
    }

    bool is_flow = (algo == "flow");
    WorkerFn worker = is_flow ? flow_join_worker : conventional_worker;

    double start_wall = wall_seconds();
    ClusterRun run = run_cluster(w, cfg, worker);
    rec.time_ms = (wall_seconds() - start_wall) * 1000.0;

    // Verify invariants after stopping the timer.
    if (is_flow) check_flow_join_run(run);

    rec.result = run.result;
    for (const Server& s : run.servers) rec.servers.push_back(s.counters);
    if (is_flow) {
        rec.heavy_r = extract_sorted_keys(run.servers[0].l_r);
        rec.heavy_s = extract_sorted_keys(run.servers[0].l_s);
    }
    return rec;
}

static RunStats compute_stats(const RunRecord& rec) {
    RunStats st;
    uint64_t total_work = 0;
    uint64_t total_cpu = 0;
    uint64_t max_cpu = 0;

    for (const Counters& c : rec.servers) {
        st.work_max = std::max(st.work_max, c.work_units);
        total_work += c.work_units;
        max_cpu = std::max(max_cpu, c.cpu_ns);
        total_cpu += c.cpu_ns;
        st.shipped += shipped_total(c);
    }

    double num_servers = static_cast<double>(rec.servers.size());
    st.work_mean = static_cast<double>(total_work) / num_servers;
    if (st.work_mean > 0.0) st.work_imbalance = st.work_max / st.work_mean;
    if (total_cpu > 0) st.cpu_imbalance = max_cpu / (static_cast<double>(total_cpu) / num_servers);
    return st;
}

// Returns the index of the lower-middle run by time_ms (or -1 if none qualify).
static int find_median_index(const std::vector<RunRecord>& runs, bool only_correct) {
    std::vector<int> indices;
    for (size_t i = 0; i < runs.size(); i++) {
        if (!only_correct || runs[i].correct) indices.push_back(static_cast<int>(i));
    }
    if (indices.empty()) return -1;

    std::sort(indices.begin(), indices.end(), [&](int a, int b) {
        return runs[a].time_ms < runs[b].time_ms;
    });
    return indices[(indices.size() - 1) / 2];
}

static std::string format_key_list(const std::vector<uint64_t>& keys) {
    if (keys.empty()) return "-";
    std::string out;
    size_t limit = std::min<size_t>(keys.size(), 8);
    for (size_t i = 0; i < limit; i++) {
        if (i > 0) out += ",";
        out += std::to_string(keys[i]);
    }
    return out;
}

static void record_and_print(FILE* results_csv, FILE* server_csv, bool& any_failed,
                             const std::string& wl_label, const std::string& algo, int n,
                             const std::vector<RunRecord>& runs, double ref_ms,
                             uint64_t total_tuples) {
    // 1. Write every repetition to results.csv.
    for (size_t i = 0; i < runs.size(); i++) {
        const RunRecord& r = runs[i];
        RunStats st = compute_stats(r);
        double ms = std::max(r.time_ms, 1e-6);
        double throughput_mtps = static_cast<double>(total_tuples) / (ms / 1000.0) / 1e6;
        double speedup = ref_ms / ms;

        std::fprintf(results_csv,
            "%s,%s,%d,%zu,%.3f,%.3f,%.3f,%llu,%.1f,%.4f,%.4f,%llu,%d\n",
            wl_label.c_str(), algo.c_str(), n, i + 1, ms, throughput_mtps, speedup,
            (unsigned long long)st.work_max, st.work_mean, st.work_imbalance,
            st.cpu_imbalance, (unsigned long long)st.shipped, r.correct ? 1 : 0);

        if (!r.correct) {
            any_failed = true;
            std::fprintf(stderr,
                "INCORRECT: %s %s n=%d rep=%zu count=%llu checksum=%llu\n",
                wl_label.c_str(), algo.c_str(), n, i + 1,
                (unsigned long long)r.result.count, (unsigned long long)r.result.checksum);
        }
    }
    std::fflush(results_csv);

    // 2. Write per-server metrics for the median repetition to per_server.csv.
    const RunRecord& med_all = runs[find_median_index(runs, false)];
    for (size_t s = 0; s < med_all.servers.size(); s++) {
        std::fprintf(server_csv, "%s,%s,%zu,%llu,%.3f\n",
            wl_label.c_str(), algo.c_str(), s,
            (unsigned long long)med_all.servers[s].work_units,
            med_all.servers[s].cpu_ns / 1e6);
    }

    // 3. Print SUMMARY line (computed over correct runs).
    int correct_count = 0;
    double min_ms = 0.0, max_ms = 0.0;
    for (const RunRecord& r : runs) {
        if (!r.correct) continue;
        if (correct_count == 0 || r.time_ms < min_ms) min_ms = r.time_ms;
        if (correct_count == 0 || r.time_ms > max_ms) max_ms = r.time_ms;
        correct_count++;
    }

    int med_ok = find_median_index(runs, true);
    if (med_ok < 0) {
        std::printf("SUMMARY %s n=%d %s no correct run correct=0/%zu\n",
                    wl_label.c_str(), n, algo.c_str(), runs.size());
    } else {
        RunStats st = compute_stats(runs[med_ok]);
        double speedup = ref_ms / std::max(runs[med_ok].time_ms, 1e-6);
        std::printf(
            "SUMMARY %s n=%d %s time_ms median=%.3f min=%.3f max=%.3f "
            "speedup=%.3f work_imb=%.3f cpu_imb=%.3f shipped=%llu correct=%d/%zu\n",
            wl_label.c_str(), n, algo.c_str(), runs[med_ok].time_ms, min_ms, max_ms,
            speedup, st.work_imbalance, st.cpu_imbalance,
            (unsigned long long)st.shipped, correct_count, runs.size());
    }

    // 4. Print Flow-Join diagnostics (heavy keys, held tuples, and SFR grids).
    if (algo == "flow") {
        uint64_t held_r = 0, held_s = 0, m_count = 0;
        for (const Counters& c : med_all.servers) {
            held_r += c.held_r;
            held_s += c.held_s;
            m_count += c.m_entering_phase3;
        }
        std::string grids;
        for (const GridInfo& g : med_all.servers[0].grids) {
            if (!grids.empty()) grids += ",";
            grids += std::to_string(g.key) + ":" + std::to_string(g.rows) + "x" + std::to_string(g.cols);
        }
        std::printf(
            "DIAG %s n=%d |L_R|=%zu |L_S|=%zu held_r=%llu held_s=%llu M=%llu "
            "grids=%s L_R=%s L_S=%s\n",
            wl_label.c_str(), n, med_all.heavy_r.size(), med_all.heavy_s.size(),
            (unsigned long long)held_r, (unsigned long long)held_s, (unsigned long long)m_count,
            grids.empty() ? "-" : grids.c_str(),
            format_key_list(med_all.heavy_r).c_str(),
            format_key_list(med_all.heavy_s).c_str());
    }
}

static void benchmark_workload(const Options& opt, const std::string& wl_name, int n,
                               FILE* results_csv, FILE* server_csv, bool& any_failed) {
    Config cfg;
    cfg.n = n;
    Workload w = build_workload(wl_name, cfg, opt);
    std::string wl_label = std::string("W") + wl_name[1];

    uint64_t total_tuples = 0;
    for (int i = 0; i < n; i++) total_tuples += w.frag_R[i].size() + w.frag_S[i].size();

    // Always run the single-threaded reference join first as oracle and speedup baseline.
    std::vector<RunRecord> ref_runs;
    for (int i = 0; i < opt.reps; i++) ref_runs.push_back(run_once("ref", w, cfg));

    const JoinResult oracle = ref_runs[0].result;
    for (RunRecord& r : ref_runs) {
        r.correct = (r.result.count == oracle.count &&
                     r.result.checksum == oracle.checksum &&
                     r.result.count == w.expected_output);
    }
    double ref_ms = ref_runs[find_median_index(ref_runs, false)].time_ms;

    for (const std::string& algo : ALL_ALGOS) {
        if (!contains(opt.algos, algo)) continue;

        std::vector<RunRecord> runs;
        if (algo == "ref") {
            runs = ref_runs;
        } else {
            for (int i = 0; i < opt.reps; i++) {
                RunRecord r = run_once(algo, w, cfg);
                r.correct = (r.result.count == oracle.count &&
                             r.result.checksum == oracle.checksum &&
                             r.result.count == w.expected_output);
                runs.push_back(std::move(r));
            }
        }
        record_and_print(results_csv, server_csv, any_failed,
                         wl_label, algo, n, runs, ref_ms, total_tuples);
    }
}

int main(int argc, char** argv) {
    Options opt = parse_args(argc, argv);

    unsigned cores = std::thread::hardware_concurrency();
    std::printf("logical cores: %u\n", cores);
    for (int n : opt.server_counts) {
        if (cores > 0 && static_cast<unsigned>(n) > cores) {
            std::fprintf(stderr, "n=%d exceeds the %u logical cores; refusing to run\n", n, cores);
            return 2;
        }
    }

    FILE* results_csv = std::fopen("results.csv", "w");
    FILE* server_csv = std::fopen("per_server.csv", "w");
    require(results_csv != nullptr && server_csv != nullptr, "cannot open the CSV files");

    std::fprintf(results_csv,
        "workload,algo,n,rep,time_ms,throughput_mtps,speedup,work_max,work_mean,"
        "work_imbalance,cpu_imbalance,shipped_tuples,correct\n");
    std::fprintf(server_csv, "workload,algo,server,work_units,cpu_ms\n");

    bool any_failed = false;
    for (const std::string& wl_name : ALL_WORKLOADS) {
        if (!contains(opt.workloads, wl_name)) continue;
        for (int n : opt.server_counts) {
            benchmark_workload(opt, wl_name, n, results_csv, server_csv, any_failed);
        }
    }

    std::fclose(results_csv);
    std::fclose(server_csv);
    return any_failed ? 1 : 0;
}