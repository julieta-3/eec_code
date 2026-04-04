
#include "Scheduler.hpp"
#include <map>
#include <set>
 
static Scheduler scheduler;
 
// Tasks waiting to be placed once a machine finishes waking up
struct PendingTask {
    VMType_t vm_type;
    CPUType_t cpu_type;
    TaskId_t task_id;
    Priority_t priority;
};

static map<MachineId_t, vector<PendingTask>> pending_tasks;
static set<MachineId_t> waking_machines;
 
 
static Priority_t set_priority(SLAType_t sla) {
    if (sla == SLA0) return HIGH_PRIORITY;
    if (sla == SLA1) return MID_PRIORITY;
    return LOW_PRIORITY;
}
 

// estimate time it will take to complete task,
static double est_completion(MachineId_t mid, uint64_t instructions) {
    MachineInfo_t info = Machine_GetInfo(mid);
    if (info.performance.empty()) return 0;
 
    double mips = info.performance[P0]; // use full speed for fair comparison
    if (mips == 0) return 0;
    double cores  = info.num_cpus;
    double active = info.active_tasks;
    if (active > cores) {
        mips = mips * cores / active;
    }
 

    return (double)instructions / mips;
}
 
// find compatible VM that meets requirements
static bool find_vm(MachineId_t machine_id, VMType_t vm_type, CPUType_t cpu,
                  vector<VMId_t>& vms, VMId_t& out_vm) {

    // check if already exists
    for (VMId_t vm_id : vms) {
        VMInfo_t info = VM_GetInfo(vm_id);
        if (info.machine_id == machine_id &&
            info.cpu == cpu &&
            info.vm_type == vm_type) {
            out_vm = vm_id;
            return true;
        }
    }

    // create new VM if can't find one
    MachineInfo_t minfo = Machine_GetInfo(machine_id);
    if (minfo.memory_size - minfo.memory_used >= 8) {
        out_vm = VM_Create(vm_type, cpu);
        VM_Attach(out_vm, machine_id);
        vms.push_back(out_vm);
        return true;
    }
    return false;
}
 

 
void Scheduler::Init() {
    unsigned total = Machine_GetTotal();
    for (unsigned i = 0; i < total; i++) {
        machines.push_back(MachineId_t(i));
    }

    for (MachineId_t mid : machines) {
        MachineInfo_t info = Machine_GetInfo(mid);
        if (info.s_state == S0) {
            VMType_t vm_type = (info.cpu == POWER) ? AIX : LINUX;
            VMId_t vm = VM_Create(vm_type, info.cpu);
            VM_Attach(vm, mid);
            vms.push_back(vm);
        }
    }
 
    SimOutput("Scheduler::Init(): " + to_string(machines.size()) +
              " machines, " + to_string(vms.size()) + " VMs", 1);
}
 
void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    TaskInfo_t task = GetTaskInfo(task_id);
    CPUType_t req_cpu = task.required_cpu;
    VMType_t req_vm = task.required_vm;
    unsigned req_mem = task.required_memory;
    Priority_t priority = set_priority(task.required_sla);
 
   // min min search
    bool found = false;
    MachineId_t best = machines[0]; // placeholder, only used when found=true
    double best_time = 0;
 
    for (MachineId_t mid : machines) {
        MachineInfo_t info = Machine_GetInfo(mid);
        //machine must be awake and CPU must match
        if (info.s_state != S0 || info.cpu != req_cpu) continue; 

        // need enough memory
        unsigned free_mem = (info.memory_size > info.memory_used)
                            ? info.memory_size - info.memory_used : 0;
        if (free_mem < req_mem + 8) continue; // 8 MB VM overhead
 
        double t = est_completion(mid, task.total_instructions);
        if (t == 0) continue; // invalid machine, skip
 

        // need machine w/ smallest completion time
        if (!found || t < best_time) {
            found  = true;
            best_time = t;
            best  = mid;
        }
    }
 

    // assign to the best
    if (found) {
        VMId_t vm;
        if (find_vm(best, req_vm, req_cpu, vms, vm)) {
            VM_AddTask(vm, task_id, priority);
            SimOutput("Scheduler::NewTask(): Task " + to_string(task_id) +
                      " -> machine " + to_string(best), 4);
            return;
        }
    }
 
    // no awake ones available so wake machine
    for (MachineId_t mid : machines) {
        if (waking_machines.count(mid)) continue;
        MachineInfo_t info = Machine_GetInfo(mid);
        if (info.s_state == S0 || info.cpu != req_cpu || info.memory_size < req_mem + 8) continue;
 
        pending_tasks[mid].push_back({req_vm, req_cpu, task_id, priority});
        waking_machines.insert(mid);
        Machine_SetState(mid, S0);
        SimOutput("Scheduler::NewTask(): Waking machine " + to_string(mid) +
                  " for task " + to_string(task_id), 3);
        return;
    }
 
    // put on machine that is alredy waking up
    for (MachineId_t mid : machines) {
        if (!waking_machines.count(mid)) continue;
        MachineInfo_t info = Machine_GetInfo(mid);
        if (info.cpu != req_cpu || info.memory_size < req_mem + 8) continue;
        pending_tasks[mid].push_back({req_vm, req_cpu, task_id, priority});
        SimOutput("Scheduler::NewTask(): Queued task " + to_string(task_id) +
                  " on waking machine " + to_string(mid), 3);
        return;
    }
 
    SimOutput("Scheduler::NewTask(): WARNING - no machine found for task " +
              to_string(task_id), 0);
}
 
void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) +
              " done at " + to_string(now), 4);
}
 
void Scheduler::PeriodicCheck(Time_t now) {

}
 
void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    SimOutput("Scheduler::MigrationComplete(): VM " + to_string(vm_id) +
              " done at " + to_string(time), 3);
}
 
void Scheduler::Shutdown(Time_t time) {
    for (VMId_t vm : vms) {
        VMInfo_t info = VM_GetInfo(vm);
        if (info.active_tasks.empty()) VM_Shutdown(vm);
    }
}
 

 
void InitScheduler() {
    scheduler.Init();
}
 
void HandleNewTask(Time_t time, TaskId_t task_id) {
    scheduler.NewTask(time, task_id);
}
 
void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    scheduler.TaskComplete(time, task_id);
}
 
void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning(): Machine " + to_string(machine_id) +
              " overcommitted at " + to_string(time), 0);
}
 
void MigrationDone(Time_t time, VMId_t vm_id) {
    scheduler.MigrationComplete(time, vm_id);
}
 
void SchedulerCheck(Time_t time) {
    scheduler.PeriodicCheck(time);
}
 
void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << " KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time) / 1000000 << " seconds" << endl;
    scheduler.Shutdown(time);
}
 
void SLAWarning(Time_t time, TaskId_t task_id) {
    // Bump the priority of the violating task so it gets more CPU time
    SetTaskPriority(task_id, HIGH_PRIORITY);
}
 

 // once awake, place tasks that were queued
void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    waking_machines.erase(machine_id);
 
    MachineInfo_t info = Machine_GetInfo(machine_id);
    if (info.s_state != S0) return; // was a sleep transition, nothing to do
 
    // machine now awake so send out tasks
    auto it = pending_tasks.find(machine_id);
    if (it == pending_tasks.end()) return;
 
    for (PendingTask& pt : it->second) {
        VMId_t vm;
        if (find_vm(machine_id, pt.vm_type, pt.cpu_type, scheduler.vms, vm)) {
            VM_AddTask(vm, pt.task_id, pt.priority);
            SimOutput("StateChangeComplete(): Placed task " + to_string(pt.task_id) +
                      " on machine " + to_string(machine_id), 3);
        }
    }
    pending_tasks.erase(it);
}
 