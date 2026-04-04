//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <algorithm>
#include <deque>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <cmath>

using namespace std;

static const double IVS_HIGH = 0.80;   
static const double IVS_LOW = 0.30;

// max tasks per VM slot for each SLA 
static const unsigned TASKS_SLA0 = 1;
static const unsigned TASKS_SLA1 = 2;
static const unsigned TASKS_SLA2 = 4;
static const unsigned TASKS_SLA3 = 4;

struct VMKey {
    MachineId_t machine;
    VMType_t vm_type;
    CPUType_t cpu;
    unsigned slot;
    bool operator<(const VMKey &o) const {
        return tie(machine, vm_type, cpu, slot)
             < tie(o.machine, o.vm_type, o.cpu, o.slot);
    }
};

struct Pending {
    TaskId_t task_id;
    Time_t arrived;
};

// global state
static vector<MachineId_t> g_machines;
static map<VMKey, VMId_t> vm_map;
static unordered_map<unsigned,unsigned> vm_load;   // task count
static set<unsigned> all_vms;
static deque<Pending> queue;
static set<TaskId_t> queued;
static bool unloading = false;


// machines set P-state based on load ratio.
// P0 = highest frequency
// load > IVS_HIGH = step toward P0 
// load < IVS_LOW = step toward Pmax 
// else stay the same
// For SLA0/SLA1 machines = P0 
static void ivs_adjust(MachineId_t m) {
    MachineInfo_t machine = Machine_GetInfo(m);
    if (machine.s_state != S0 || machine.p_states.empty()) return;

    int cur = (int)machine.p_state;
    int maxp = (int)machine.p_states.size() - 1;
    int want = cur;

    if (machine.active_tasks == 0) {
        want = maxp;
    } else {
        double ncpus = (double)(machine.num_cpus > 0 ? machine.num_cpus : 1);
        double load = (double)machine.active_tasks / ncpus;

        if (load >= IVS_HIGH) {
            want = max(0, cur - 1);
        } else if (load < IVS_LOW) {
            want = min(maxp, cur + 1);
        }
    }
    if (want != cur)
        Machine_SetCorePerformance(m, 0, (CPUPerformance_t)want);

}

// SLA0/1: small least load, fastest
// SLA3: lightl load, idle at low P-state.
// high loads good more idle
static double score(MachineId_t m, TaskId_t tid) {
    MachineInfo_t machine = Machine_GetInfo(m);
    TaskInfo_t task = GetTaskInfo(tid);

    if (machine.s_state != S0) return INFINITY;
    if (Machine_GetCPUType(m) != task.required_cpu) return INFINITY;

    double ncpus = (machine.num_cpus > 0 ? machine.num_cpus : 1);
    double load = machine.active_tasks / ncpus;

    if (task.required_sla == SLA0) {
        if (machine.active_tasks >= machine.num_cpus) return INFINITY;
        return load;
    }
    if (task.required_sla == SLA1) {
        if (machine.active_tasks >= machine.num_cpus * 2u) return INFINITY;
        return load;
    }
    if (task.required_sla == SLA2) {
        return load;
    }
    return 1.0 - load;
}

// VM management
static VMId_t get_vm(MachineId_t m, TaskId_t tid) {
    TaskInfo_t task;
    unsigned slots;
    task = GetTaskInfo(tid);
    slots = Machine_GetInfo(m).num_cpus;

    SLAType_t sla = task.required_sla;
    unsigned max_tasks = TASKS_SLA3;
    if (sla == SLA0) {
        max_tasks = TASKS_SLA0;
    } else if (sla == SLA1) {
        max_tasks = TASKS_SLA1;
    } else if (sla == SLA2) {
        max_tasks = TASKS_SLA2;
    }

    for (unsigned s = 0; s < slots; ++s) {
        VMKey key{ m, task.required_vm, task.required_cpu, s };
        auto  iterator = vm_map.find(key);

        if (iterator == vm_map.end()) {
            bool enough_memory = false;
            MachineInfo_t machine = Machine_GetInfo(m);
            if (machine.memory_used < machine.memory_size) {
                unsigned mem_needed = GetTaskInfo(tid).required_memory + 16u;
                enough_memory = (machine.memory_size - machine.memory_used) >= mem_needed;
            }

            if (!enough_memory) continue;
            VMId_t vm = VM_Create(task.required_vm, task.required_cpu);
            VM_Attach(vm, m);
            vm_map[key] = vm;
            vm_load[(unsigned)vm] = 0;
            all_vms.insert((unsigned)vm);
            return vm;
        }

        VMId_t vm = iterator->second;
        unsigned &ld = vm_load[(unsigned)vm];
        bool enough_memory = false;
        MachineInfo_t machine = Machine_GetInfo(m);
        if (machine.memory_used < machine.memory_size) {
            unsigned mem_needed = GetTaskInfo(tid).required_memory + 16u;
            enough_memory = (machine.memory_size - machine.memory_used) >= mem_needed;
        }
        if (ld < max_tasks && enough_memory)
            return vm;
    }
    return VMId_t(-1);
}

// placement
static bool place(TaskId_t tid) {
    TaskInfo_t task_info;
    task_info = GetTaskInfo(tid);

    struct C { MachineId_t m; double s; };
    vector<C> candidates;
    candidates.reserve(g_machines.size());
    for (MachineId_t m : g_machines) {
        double s = score(m, tid);
        if (!isinf(s)) candidates.push_back({m, s});
    }
    sort(candidates.begin(), candidates.end(), [](const C &a, const C &b){ return a.s < b.s; });

    // goes through sorted machines and tries to place on best match
    // find or create VM slot
    for (auto &c : candidates) {
        VMId_t vm = get_vm(c.m, tid);
        if (vm == VMId_t(-1)) continue;
        Priority_t priority = LOW_PRIORITY;
        if (task_info.required_sla == SLA0) { 
            priority = HIGH_PRIORITY; 
        } else if (task_info.required_sla == SLA1) {
            priority = MID_PRIORITY;
        }
        VM_AddTask(vm, tid, priority);
        vm_load[(unsigned)vm]++;
        // increments task count and sets p-state
        ivs_adjust(c.m);
        return true;
    }
    return false;
}

// places taks in queues
static void place_pending() {
    if (queue.empty() || unloading) return;
    unloading = true;

    stable_sort(queue.begin(), queue.end(),
        [](const Pending &a, const Pending &b){
            int priority_a = (int)RequiredSLA(a.task_id);
            int priority_b = (int)RequiredSLA(b.task_id);
            return priority_a != priority_b ? priority_a < priority_b : a.arrived < b.arrived;
        });

    deque<Pending> leftover;
    for (auto &p : queue) {
        bool done = false;
        done = IsTaskCompleted(p.task_id);
        if (done) { queued.erase(p.task_id); continue; }

        if (place(p.task_id)) {
            queued.erase(p.task_id);
        } else {
            leftover.push_back(p);
        }
    }
    queue = move(leftover);
    unloading = false;
}


void Scheduler::Init() {
    g_machines.clear();
    vm_map.clear();
    vm_load.clear();
    all_vms.clear();
    queue.clear();
    queued.clear();
    unloading = false;

    machines.clear();
    vms.clear();

    unsigned total = (unsigned)Machine_GetTotal();
    SimOutput("Scheduler::Init() total=" + to_string(total), 1);

    for (unsigned i = 0; i < total; ++i) {
        MachineId_t m = MachineId_t(i);
        g_machines.push_back(m);
        machines.push_back(m);
        // all machines S0 at startup
        Machine_SetState(m, S0); 
        if (!Machine_GetInfo(m).p_states.empty())
            Machine_SetCorePerformance(m, 0, (CPUPerformance_t)0);
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t tid) {
    if (!place(tid) && !(queued.count(tid))) {
        queue.push_back({tid, now});
        queued.insert(tid);
    }
    place_pending();
}

void Scheduler::TaskComplete(Time_t /*t*/, TaskId_t /*tid*/) {
    for (unsigned vid : all_vms) {
        // try incase VM destroyed
        try {
            vm_load[vid] = (unsigned)VM_GetInfo(VMId_t(vid)).active_tasks.size();
        } catch (...) {
            vm_load[vid] = 0;
        }
    }
    place_pending();
    for (MachineId_t m : g_machines) ivs_adjust(m);   // calc machines' P-states when load changes
}

void Scheduler::PeriodicCheck(Time_t /*t*/) {
    for (unsigned vid : all_vms) {
        try {
            vm_load[vid] = (unsigned)VM_GetInfo(VMId_t(vid)).active_tasks.size();
        } catch (...) {
            vm_load[vid] = 0;
        }
    }
    place_pending();
    for (MachineId_t m : g_machines) ivs_adjust(m);  
}

void Scheduler::MigrationComplete(Time_t /*t*/, VMId_t /*v*/) {
    place_pending();
}

void Scheduler::Shutdown(Time_t /*t*/) {
    for (unsigned vid : all_vms) {
        if (VM_GetInfo(VMId_t(vid)).active_tasks.empty())
            VM_Shutdown(VMId_t(vid));
    }
}

// simulator calls
static Scheduler g_sched;

void InitScheduler() {
    SimOutput("InitScheduler()", 4);
    g_sched.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask() id=" + to_string(task_id), 4);
    g_sched.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion() id=" + to_string(task_id), 4);
    g_sched.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning() machine=" + to_string(machine_id), 0);
    g_sched.PeriodicCheck(time);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone() vm=" + to_string(vm_id), 4);
    g_sched.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time) {
    SimOutput("SchedulerCheck()", 4);
    g_sched.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    g_sched.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    // task abt to miss deadline
    // boost priority, P0 on busy machines — avoid looking for machine with task
    SimOutput("SLAWarning() id=" + to_string(task_id), 2);
    SetTaskPriority(task_id, HIGH_PRIORITY); 

    for (MachineId_t m : g_machines) {
        MachineInfo_t machine = Machine_GetInfo(m);
        if (machine.active_tasks > 0 && machine.s_state == S0 && (int)machine.p_state != 0) {
            Machine_SetCorePerformance(m, 0, (CPUPerformance_t)0);
        }
    }
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // place pending in case tasks arrived before machines ready
    SimOutput("StateChangeComplete() machine=" + to_string(machine_id), 4);
    place_pending();
}