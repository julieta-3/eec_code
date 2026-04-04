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

static const unsigned TASKS_SLA0 = 1;   // max tasks per VM for SLA0
static const unsigned TASKS_SLA1 = 2;
static const unsigned TASKS_SLA2 = 4;
static const unsigned TASKS_SLA3 = 4;

struct VMKey {
    MachineId_t machine;
    VMType_t vm_type;
    CPUType_t cpu;
    unsigned slot;     
    bool operator<(const VMKey &o) const {
        return tie(machine, vm_type, cpu, slot) < tie(o.machine, o.vm_type, o.cpu, o.slot);
    }
};

struct Pending {
    TaskId_t task_id;
    Time_t arrived;
};

// machine state
static vector<MachineId_t> g_machines;
static map<VMKey, VMId_t> vm_map;
static unordered_map<unsigned,unsigned> vm_load;   // num of active tasks
static set<unsigned> all_vms;
static deque<Pending> queue;
static set<TaskId_t> tasks_queued;
static bool pending = false;

// p-state = P0 when busy, lowest when idle.
// set CPU performance based on load
static void throttle(MachineId_t m) {
    
    MachineInfo_t info = Machine_GetInfo(m);
    if (info.s_state != S0 || info.p_states.empty()) return;

    int max_pstate = (int)info.p_states.size() - 1;
    double ncpus = (info.num_cpus > 0) ? info.num_cpus : 1.0;
    double load = (double)info.active_tasks / ncpus;

    int want = max_pstate;
    if (info.active_tasks == 0) {
        want = max_pstate;
    } else if (load > 0.75) {
        want = 0;
    } else if (load > 0.25) {
        want = min(1, max_pstate);
    } else {
        want = max_pstate;
    }

    if ((int)info.p_state != want)
        Machine_SetCorePerformance(m, 0, (CPUPerformance_t)want);
}

// find/create a VM on machine m suitable for task
static VMId_t get_vm(MachineId_t m, TaskId_t tid) {
    TaskInfo_t task = GetTaskInfo(tid);
    MachineInfo_t machine = Machine_GetInfo(m);

    unsigned num_cpus = machine.num_cpus;
    unsigned max_tasks = TASKS_SLA3;
    if (task.required_sla == SLA0) {
        max_tasks = TASKS_SLA0;
    } else if (task.required_sla == SLA1) {
        max_tasks = TASKS_SLA1;
    } else if (task.required_sla == SLA2) {
        max_tasks = TASKS_SLA2;
    }

    unsigned mem_for_new_vm = task.required_memory + 16u;
    unsigned mem_for_existing_vm = task.required_memory;

    for (unsigned s = 0; s < num_cpus; s++) {
        VMKey key{m, task.required_vm, task.required_cpu, s};
        auto iterator = vm_map.find(key);

        if (iterator != vm_map.end()) {
            VMId_t vm = iterator->second;
            unsigned cur_load = vm_load[(unsigned)vm];

            bool enough_mem =
                machine.memory_used < machine.memory_size &&
                (machine.memory_size - machine.memory_used) >= mem_for_existing_vm;

            if (cur_load < max_tasks && enough_mem) {
                return vm;
            }
        } else {
            bool enough_mem =
                machine.memory_used < machine.memory_size &&
                (machine.memory_size - machine.memory_used) >= mem_for_new_vm;

            if (!enough_mem) {
                continue;
            }

            VMId_t vm = VM_Create(task.required_vm, task.required_cpu);
            VM_Attach(vm, m);
            vm_map[key] = vm;
            vm_load[(unsigned)vm] = 0;
            all_vms.insert((unsigned)vm);
                return vm;
        }
    }


    return VMId_t(-1);
}

// ranks based on how good a machine is for a specific task 
// score based on load, SLA, GPU and mem
// lower = better fit
static double score(MachineId_t m, TaskId_t tid) {
    MachineInfo_t machine = Machine_GetInfo(m);
    TaskInfo_t task = GetTaskInfo(tid);

    // machine must be on and match CPU type
    if (machine.s_state != S0) return INFINITY;
    if (Machine_GetCPUType(m) != task.required_cpu) return INFINITY;

    double ncpus = (machine.num_cpus > 0) ? machine.num_cpus : 1.0;
    double load = (double)machine.active_tasks / ncpus;

    // GPU preference
    double gpu_bonus = 0.0;
    if (task.gpu_capable && machine.gpus) {
        gpu_bonus = 0.1;
    }

    // check available memory
    double free_mem = 0.0;
    if (machine.memory_size > machine.memory_used) {
        free_mem = (double)(machine.memory_size - machine.memory_used);
    }

    double mem_penalty = 0.0;
    if (free_mem < 256.0) {
        mem_penalty = 0.1;
    }

    if (task.required_sla == SLA0) {
        if (machine.active_tasks >= machine.num_cpus) return INFINITY;
        return load - gpu_bonus + mem_penalty;
    }

    if (task.required_sla == SLA1) {
        if (machine.active_tasks >= machine.num_cpus * 2u) return INFINITY;
        return load - gpu_bonus + mem_penalty;
    }

    if (task.required_sla == SLA2) {
        if (load > 0.85) return INFINITY;
        return (1.0 - load) - gpu_bonus + mem_penalty;
    }

    if (load > 0.95) return INFINITY;
    return (1.0 - load) - gpu_bonus + mem_penalty;
}

// place task, true if successful
static bool place(TaskId_t tid) {
    TaskInfo_t task_info;
    task_info = GetTaskInfo(tid);

    Priority_t priority = LOW_PRIORITY;
    if (task_info.required_sla == SLA0) priority = HIGH_PRIORITY;
    else if (task_info.required_sla == SLA1) priority = MID_PRIORITY;

    MachineId_t best = MachineId_t(-1);
    double best_score = INFINITY;

    for (MachineId_t m : g_machines) {
        double s = score(m, tid);
        if (!isinf(s) && s < best_score) {
            best_score = s;
            best = m;
        }
    }

    if (best != MachineId_t(-1)) {
        VMId_t vm = get_vm(best, tid);
        if (vm != VMId_t(-1)) {
            VM_AddTask(vm, tid, priority);
            vm_load[(unsigned)vm]++;
            throttle(best);
            return true;
        }
    }

    for (MachineId_t m : g_machines) {
        if (m == best) continue;
        double s = score(m, tid);
        if (isinf(s)) continue;

        VMId_t vm = get_vm(m, tid);
        if (vm == VMId_t(-1)) continue;

        VM_AddTask(vm, tid, priority);
        vm_load[(unsigned)vm]++;
        throttle(m);
        return true;
    }

    return false;
}

// run queued tasks, based on SLA
static void place_pending() {
    if (queue.empty() || pending) return;
    pending = true;

    deque<Pending> leftover;

    // first urgent tasks
    for (auto &p : queue) {
        bool done = false;
        try {
            done = IsTaskCompleted(p.task_id);
        } catch (...) {
            done = true;
        }

        if (done) {
            tasks_queued.erase(p.task_id);
            continue;
        }

        SLAType_t sla;
        try {
            sla = RequiredSLA(p.task_id);
        } catch (...) {
            leftover.push_back(p);
            continue;
        }

        if (sla == SLA0 || sla == SLA1) {
            if (place(p.task_id)) {
                tasks_queued.erase(p.task_id);
            } else {
                leftover.push_back(p);
            }
        } else {
            leftover.push_back(p);
        }
    }

    queue = move(leftover);
    leftover.clear();

    // lower-priority tasks
    for (auto &p : queue) {
        bool done = false;
        try {
            done = IsTaskCompleted(p.task_id);
        } catch (...) {
            done = true;
        }

        if (done) {
            tasks_queued.erase(p.task_id);
            continue;
        }

        if (place(p.task_id)) {
            tasks_queued.erase(p.task_id);
        } else {
            leftover.push_back(p);
        }
    }

    queue = move(leftover);
    pending = false;
}

void Scheduler::Init() {
    g_machines.clear();
    vm_map.clear();
    vm_load.clear();
    all_vms.clear();
    queue.clear();
    tasks_queued.clear();
    pending = false;

    machines.clear();
    vms.clear();

    unsigned total = (unsigned)Machine_GetTotal();
    SimOutput("Scheduler::Init() total=" + to_string(total), 1);

    for (unsigned i = 0; i < total; i++) {
        MachineId_t m = MachineId_t(i);
        g_machines.push_back(m);
        machines.push_back(m);

        Machine_SetState(m, S0);
        MachineInfo_t info = Machine_GetInfo(m);
        if (!info.p_states.empty()) {
            int low = (int)info.p_states.size() - 1;
            Machine_SetCorePerformance(m, 0, (CPUPerformance_t)low);
        }
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t tid) {
    if (!place(tid) && !(tasks_queued.count(tid))) {
        queue.push_back({tid, now});
        tasks_queued.insert(tid);
    }
    place_pending();
}

void Scheduler::TaskComplete(Time_t /*t*/, TaskId_t /*tid*/) {
    for (unsigned vid : all_vms) {
        try {
            vm_load[vid] = (unsigned)VM_GetInfo(VMId_t(vid)).active_tasks.size();
        } catch (...) {
            vm_load[vid] = 0;
        }
    }
    place_pending();
    for (MachineId_t m : g_machines) throttle(m);
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
    for (MachineId_t m : g_machines) throttle(m);
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
    // update VM loads, place pending, adjust performance
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
    SimOutput("SLAWarning() id=" + to_string(task_id), 2);

    SetTaskPriority(task_id, HIGH_PRIORITY);

    for (MachineId_t m : g_machines) {
        MachineInfo_t info = Machine_GetInfo(m);
        if (info.s_state != S0) continue;
        if (info.active_tasks == 0) continue;
        if (info.p_states.empty()) continue;

        if ((int)info.p_state != 0) {
            Machine_SetCorePerformance(m, 0, (CPUPerformance_t)0);
        }
    }
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // check pending taks in case tasks arrived before machines ready
    SimOutput("StateChangeComplete() machine=" + to_string(machine_id), 4);
    place_pending();
}