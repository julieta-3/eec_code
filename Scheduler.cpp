

// Greedy
#include "Scheduler.hpp"
#include <map>
#include <algorithm>
 
struct PendingTask {
    VMType_t vm_type;
    CPUType_t cpu_type;
    TaskId_t task_id;
    Priority_t priority;
};
 
static map<MachineId_t, PendingTask> pending_tasks;
static map<TaskId_t, VMId_t> task_to_vm;
static map<MachineId_t, bool> transitioning;
static map<VMId_t, bool> vm_migrating; // VM between machines
static map<MachineId_t, bool> shutting_down;
static map<VMId_t, bool> vm_shutdown; 
static map<CPUType_t, unsigned> tasks_seen;
static map<MachineId_t, unsigned> migration_dest_count; // in progress of migrating
 
static const unsigned MAX_TASKS_PER_VM = 2;
static const unsigned MAX_TASKS_SLA0 = 1;
static const unsigned MIN_ACTIVE_PER_CPU = 4;
 
static Scheduler* sched = nullptr;
 

 // for sorting machines
static double machine_util(MachineId_t m) {
    MachineInfo_t info = Machine_GetInfo(m);
    if (info.memory_size == 0) return 0.0;
    return (double)info.memory_used / info.memory_size;
}
 

bool check_vm(VMId_t vm, MachineId_t machine_id, CPUType_t cpu, VMType_t vm_type, unsigned cap) {
    if (vm_shutdown[vm] || vm_migrating[vm]) return false;  // vm busy or shutting down
    VMInfo_t info = VM_GetInfo(vm);
    if (info.machine_id != machine_id) return false;
    if (info.cpu != cpu) return false;
    if (info.vm_type != vm_type) return false;
    if (info.active_tasks.size() >= cap) return false;
    return true;
}

 // find a VM with the least load but still upder the cap
static VMId_t find_vm(MachineId_t machine_id, CPUType_t cpu, VMType_t vm_type, unsigned cap) {
    VMId_t best = VMId_t(-1); // will return this if not found 
    unsigned min_tasks = cap + 1;


    for (VMId_t vm : sched->vms) {
        if (!check_vm(vm, machine_id, cpu, vm_type, cap)) continue;

        unsigned active_count = VM_GetInfo(vm).active_tasks.size();
        if (active_count < min_tasks) {
            min_tasks = active_count;
            best = vm;
        }
    }
    return best;
}


 
 // attach the VM to machine, safety checks for sleeping machines!
static bool attach_vm(VMId_t vm, MachineId_t m) {
    // MachineInfo_t info = Machine_GetInfo(m);
    if (shutting_down.count(m)) return false;
    if (migration_dest_count.count(m) && migration_dest_count[m] > 0) return false;
    if (Machine_GetInfo(m).s_state != S0) return false;
    VM_Attach(vm, m);
    return true;
}
 
 // move VM to other machine, lots of checks for no crashes
static bool migrate_vm(VMId_t vm, MachineId_t dest) {
    if ((vm_migrating.count(vm) && vm_migrating[vm]) || 
        (vm_shutdown.count(vm) && vm_shutdown[vm])) return false;

    VMInfo_t vm_info = VM_GetInfo(vm);
    if (vm_info.machine_id == dest || vm_info.active_tasks.empty()) return false;
    if (shutting_down.count(dest) || transitioning.count(dest)) return false;

    MachineInfo_t dest_info = Machine_GetInfo(dest);
    if (dest_info.s_state != S0 || dest_info.cpu != vm_info.cpu) return false;
    
    // if machine is idle, don't migrate because may be shut off by the time migration finishes
    if (dest_info.active_tasks == 0 && dest_info.memory_used <= 8) return false;

    MachineInfo_t info = Machine_GetInfo(vm_info.machine_id);
    if (dest_info.memory_size - dest_info.memory_used < dest_info.memory_used) return false;
    VM_Migrate(vm, dest);
    vm_migrating[vm] = true;
    migration_dest_count[dest]++;
    return true;
}
 
static void shutdown_vm(VMId_t vm) {
    if (vm_shutdown.count(vm) && vm_shutdown[vm]) return;
    if (!VM_GetInfo(vm).active_tasks.empty()) return;
    vm_shutdown[vm] = true;
    VM_Shutdown(vm);
}
 
static unsigned num_active_machines(CPUType_t cpu) {
    unsigned n = 0;
    for (MachineId_t m : sched->machines) {
        if (shutting_down.count(m)) continue;
        MachineInfo_t info = Machine_GetInfo(m);
        if (info.s_state == S0 && info.cpu == cpu) n++;
    }
    return n;
}
 

 // don't shutdown unless passes the checks
static bool shutdown_ready(MachineId_t m) {
    if (shutting_down.count(m) || pending_tasks.count(m) || transitioning.count(m)) return false; 
    if (migration_dest_count.count(m) && migration_dest_count[m] > 0) return false;
    MachineInfo_t info = Machine_GetInfo(m);

    if (info.active_tasks > 0 || tasks_seen[info.cpu] == 0) return false; // runing tasks


    // check that there aren't any VMs or currently migrating 
    if (num_active_machines(info.cpu) <= MIN_ACTIVE_PER_CPU) return false;
    for (VMId_t vm : sched->vms) {
        if (vm_shutdown.count(vm) && vm_shutdown[vm]) continue;
        VMInfo_t vm_info = VM_GetInfo(vm);
        if (vm_info.machine_id != m) continue;
        if (!vm_info.active_tasks.empty()) return false;
        if (vm_migrating.count(vm) && vm_migrating[vm]) return false;
    }
    return true;
}
 

// place task on a machine, sort so best first
static bool place_task(TaskId_t task_id, CPUType_t cpu, VMType_t vm_type,
                  unsigned mem, Priority_t priority, SLAType_t sla, unsigned cap) {
    vector<MachineId_t> active;
    for (MachineId_t m : sched->machines) {
        MachineInfo_t info = Machine_GetInfo(m);
        if (info.s_state != S0 || info.cpu != cpu) continue;
        if (shutting_down.count(m)) continue;
        active.push_back(m);
    }


    //SLA0 on empty machines, task gets full CPU util of machine
    // SLAs sort by tasks count followd by mem. util.

    // https://www.geeksforgeeks.org/cpp/sort-c-stl/
    sort(active.begin(), active.end(), [sla](MachineId_t a, MachineId_t b) {
        MachineInfo_t a_info = Machine_GetInfo(a);
        MachineInfo_t b_info = Machine_GetInfo(b);
        if (sla == SLA0) {
            bool a_empty = (a_info.active_tasks == 0);
            bool b_empty = (b_info.active_tasks == 0);
            if (a_empty != b_empty) return a_empty > b_empty;
        }
        if (a_info.active_tasks != b_info.active_tasks)
            return a_info.active_tasks < b_info.active_tasks;
        return machine_util(a) < machine_util(b);
    });

    for (MachineId_t m : active) {
        MachineInfo_t m_info = Machine_GetInfo(m);
        unsigned free = m_info.memory_size - m_info.memory_used;
        if (free < mem) continue;
        VMId_t vm = find_vm(m, cpu, vm_type, cap);
        if (vm != VMId_t(-1)) {
            VM_AddTask(vm, task_id, priority);
            task_to_vm[task_id] = vm;
            return true;
        }
        // no reusable VM so make new one but check overhead
        if (free >= mem + 8) {
            VMId_t new_vm = VM_Create(vm_type, cpu);
            if (!attach_vm(new_vm, m)) continue;
            VM_AddTask(new_vm, task_id, priority);
            sched->vms.push_back(new_vm);
            task_to_vm[task_id] = new_vm;
            return true;
        }
    }
    return false;
}
 
 
void Scheduler::Init() {
    unsigned total = Machine_GetTotal();
    SimOutput("Scheduler::Init(): total machines = " + to_string(total), 0);
    for (unsigned i = 0; i < total; i++)
        machines.push_back(MachineId_t(i));
    SimOutput("Scheduler::Init(): done", 0);
}
 
void Scheduler::MigrationComplete(Time_t time, VMId_t vm) {
    vm_migrating[vm] = false;
    VMInfo_t info = VM_GetInfo(vm);
    if (migration_dest_count.count(info.machine_id) && migration_dest_count[info.machine_id] > 0)
        migration_dest_count[info.machine_id]--;
}
 
void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    TaskInfo_t task = GetTaskInfo(task_id);
    CPUType_t cpu = task.required_cpu;
    VMType_t vmt = task.required_vm;
    unsigned mem = task.required_memory;
    SLAType_t sla = task.required_sla;
    Priority_t pri = (sla == SLA0) ? HIGH_PRIORITY :
                      (sla == SLA1) ? MID_PRIORITY  : LOW_PRIORITY;
    tasks_seen[cpu]++;
 
        // machine already awake
    unsigned cap = (sla == SLA0) ? MAX_TASKS_SLA0 : MAX_TASKS_PER_VM;
    if (place_task(task_id, cpu, vmt, mem, pri, sla, cap)) return;
 

    // wake up a machine and queue task for once it is awake
    for (MachineId_t m : machines) {
        MachineInfo_t m_info = Machine_GetInfo(m);
        if (m_info.s_state == S0 || m_info.cpu != cpu || m_info.memory_size < mem + 8) continue; 
        if (transitioning.count(m) || shutting_down.count(m)) continue; 
        pending_tasks[m] = {vmt, cpu, task_id, pri};
        transitioning[m] = true;
        Machine_SetState(m, S0);
        return;
    }

    // machine w/ no pending task yet
    for (MachineId_t m : machines) {
        MachineInfo_t m_info = Machine_GetInfo(m);
        if (m_info.cpu != cpu) continue;
        if (!transitioning.count(m) || shutting_down.count(m) || pending_tasks.count(m)) continue;
        if (m_info.memory_size < mem + 8) continue;
        pending_tasks[m] = {vmt, cpu, task_id, pri};
        return;
    }
    
    // at capacity, relax until place is found
    for (unsigned fallback = MAX_TASKS_PER_VM + 1; fallback <= 8; fallback++) {
        if (place_task(task_id, cpu, vmt, mem, pri, sla, fallback)) return;
    }
 
    SimOutput("NewTask(): WARNING could not place task " + to_string(task_id), 0);
}


 
void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    task_to_vm.erase(task_id);
    
    // only shut down if the minimum is met
    vector<MachineId_t> active;
    for (MachineId_t m : machines) {
        MachineInfo_t info = Machine_GetInfo(m);
        if (info.s_state == S0 && !shutting_down.count(m))
            active.push_back(m);
    }
    if (active.size() <= MIN_ACTIVE_PER_CPU) return;
 
    // shut down least loaded to save energy
    sort(active.begin(), active.end(), [](MachineId_t a, MachineId_t b) {
        return machine_util(a) < machine_util(b);
    });

    MachineId_t src = active.front();
    if (shutdown_ready(src)) {
        for (VMId_t vm : vms) {
            if (vm_shutdown.count(vm) && vm_shutdown[vm]) continue;
            VMInfo_t v_info = VM_GetInfo(vm);
            if (v_info.machine_id != src) continue;
            if (!v_info.active_tasks.empty()) continue;
            shutdown_vm(vm);
        }
        shutting_down[src] = true;
        Machine_SetState(src, S5);
    }
}
 
void Scheduler::PeriodicCheck(Time_t now) {}
 
void Scheduler::Shutdown(Time_t time) {
    for (VMId_t vm : vms) shutdown_vm(vm);
    SimOutput("Shutdown(): done at " + to_string(time), 4);
}

 
void InitScheduler() {
    SimOutput("InitScheduler(): starting", 0);
    sched = new Scheduler();
    sched->Init();
}
 
void HandleNewTask(Time_t time, TaskId_t task_id) { sched->NewTask(time, task_id); }
void HandleTaskCompletion(Time_t time, TaskId_t task_id) { sched->TaskComplete(time, task_id); }
 

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    static map<MachineId_t, Time_t> last_handled;
    if (last_handled.count(machine_id) && last_handled[machine_id] == time) return;
    last_handled[machine_id] = time;
    MachineInfo_t src_info = Machine_GetInfo(machine_id);
    vector<MachineId_t> candidates;
    for (MachineId_t m : sched->machines) {
        if (m == machine_id) continue;
        if (shutting_down.count(m) || transitioning.count(m) ) continue;
        MachineInfo_t info = Machine_GetInfo(m);
        if (info.s_state != S0 || info.cpu != src_info.cpu) continue;
        candidates.push_back(m);
    }
    sort(candidates.begin(), candidates.end(), [](MachineId_t a, MachineId_t b) {
        return machine_util(a) < machine_util(b);
    });


    // overloaded so migrate the VM
    for (VMId_t vm : sched->vms) {
        if ((vm_shutdown.count(vm) && vm_shutdown[vm]) || 
        (vm_migrating.count(vm) && vm_migrating[vm])) continue;
        VMInfo_t v_info = VM_GetInfo(vm);
        if (v_info.machine_id != machine_id || v_info.active_tasks.empty() ) continue;
        for (MachineId_t dest : candidates)
            if (migrate_vm(vm, dest)) return;
    }
}
 
void MigrationDone(Time_t time, VMId_t vm_id) { sched->MigrationComplete(time, vm_id); }
void SchedulerCheck(Time_t time) { sched->PeriodicCheck(time); }
 
void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << " KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    sched->Shutdown(time);
}
 
void SLAWarning(Time_t time, TaskId_t task_id) {
    auto entry = task_to_vm.find(task_id);
    if (entry == task_to_vm.end()) return;
    VMId_t vm = entry->second;
    if ((vm_shutdown.count(vm)  && vm_shutdown[vm]) || 
        (vm_migrating.count(vm) && vm_migrating[vm]))  return;

    // increase priority
    SetTaskPriority(task_id, HIGH_PRIORITY);

    // SLA2 deadline
    TaskInfo_t task_info = GetTaskInfo(task_id);
    if (task_info.required_sla == SLA2) return;  // migration too slow for SLA2

    // SLA0/1 migrate to less-loaded
    VMInfo_t vm_info = VM_GetInfo(vm);
    MachineId_t src = vm_info.machine_id;
    MachineInfo_t m_info = Machine_GetInfo(src);

    vector<MachineId_t> candidates;
    for (MachineId_t m : sched->machines) {
        MachineInfo_t info = Machine_GetInfo(m);
        if (m == src) continue;
        if (info.s_state != S0) continue;
        if (shutting_down.count(m) || transitioning.count(m))  continue;
        if (info.cpu != m_info.cpu)      continue;
        candidates.push_back(m);
    }

    sort(candidates.begin(), candidates.end(), [](MachineId_t a, MachineId_t b) {
        return machine_util(a) < machine_util(b);
    });


    for (MachineId_t dest : candidates)
        if (migrate_vm(vm, dest)) return;

    // no active active available so wake one up
    for (MachineId_t m : sched->machines) {
        MachineInfo_t info = Machine_GetInfo(m);
        if (info.s_state == S0 || info.cpu != m_info.cpu) continue;
        if (transitioning.count(m) || shutting_down.count(m)) continue;
        transitioning[m] = true;
        Machine_SetState(m, S0);
        return;
    }
}
 
void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    MachineInfo_t m_info = Machine_GetInfo(machine_id);
    if (m_info.s_state != S0) {
        // start clean up
        shutting_down.erase(machine_id);
        transitioning.erase(machine_id);
        return;
    }

    transitioning.erase(machine_id);
    shutting_down.erase(machine_id);
    auto entry = pending_tasks.find(machine_id);
    if (entry == pending_tasks.end()) return;
    PendingTask& pt = entry->second;

    // make sure state hasn't changed
    if (Machine_GetInfo(machine_id).s_state != S0 || shutting_down.count(machine_id)) {
        pending_tasks.erase(entry);
        return;
    }
    VMId_t nvm = VM_Create(pt.vm_type, pt.cpu_type);
    if (attach_vm(nvm, machine_id)) {
        VM_AddTask(nvm, pt.task_id, pt.priority);
        sched->vms.push_back(nvm);
        task_to_vm[pt.task_id] = nvm;
    }
    pending_tasks.erase(entry);
}