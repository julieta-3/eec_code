//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <map>

static bool migrating = false;

struct PendingTask {
    VMType_t vm_type;
    CPUType_t cpu_type;
    TaskId_t task_id;
    Priority_t priority;
};
static map<MachineId_t, PendingTask> pending_tasks;

void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);

    // sara
    unsigned total_machines = Machine_GetTotal();
    for(unsigned i = 0; i < total_machines; i++) {
        machines.push_back(MachineId_t(i));
    }
    
    for (unsigned i = 0; i < total_machines; i++) {
        MachineInfo_t info = Machine_GetInfo(machines[i]);
        if (info.s_state == S0) {
            VMType_t vm_type;
            if (info.cpu == POWER) {
                vm_type = AIX;
            } else {
                vm_type = LINUX;
            }
            VMId_t vm = VM_Create(vm_type, info.cpu);
            VM_Attach(vm, MachineId_t(i));
            vms.push_back(vm);
        }
        
    }

    SimOutput("Scheduler::Init(): VM ids are " + to_string(vms[0]) + " ahd " + to_string(vms[1]), 3);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
    SimOutput("Scheduler::MigrationComplete(): VM " + to_string(vm_id) + " migration done at " + to_string(time), 3);
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get the task parameters
    TaskInfo_t task = GetTaskInfo(task_id);

    CPUType_t  required_cpu    = task.required_cpu;
    VMType_t   required_vm     = task.required_vm;
    unsigned   required_memory = task.required_memory;
    SLAType_t  sla             = task.required_sla;

    // priorities
    Priority_t priority;
    if (sla == SLA0) {
        priority = HIGH_PRIORITY;
    } else if (sla == SLA1) {
        priority = MID_PRIORITY;
    } else {
        priority = LOW_PRIORITY;
    }

    // sara add to existing vm 
    for (VMId_t vm_id : vms) {
        VMInfo_t vm_info = VM_GetInfo(vm_id);
        if (vm_info.cpu != required_cpu) continue;
        if (vm_info.vm_type != required_vm) continue;
        MachineInfo_t machine_info = Machine_GetInfo(vm_info.machine_id);
        if (machine_info.s_state != S0) continue;
        unsigned mem_available = machine_info.memory_size - machine_info.memory_used;
        if (mem_available < required_memory) continue;
        VM_AddTask(vm_id, task_id, priority);
        SimOutput("Scheduler::NewTask(): Task " + to_string(task_id) + " added to existing VM " + to_string(vm_id), 4);
        return;
    }

    // sara create new VM on active machine
    for (MachineId_t machine_id : machines) {
        MachineInfo_t machine_info = Machine_GetInfo(machine_id);
        if (machine_info.s_state != S0) continue;
        if (machine_info.cpu != required_cpu) continue;
 
        unsigned mem_available = machine_info.memory_size - machine_info.memory_used;
        if (mem_available < required_memory + 8) continue; 
 
        VMId_t new_vm = VM_Create(required_vm, required_cpu);
        VM_Attach(new_vm, machine_id);
        VM_AddTask(new_vm, task_id, priority);
        vms.push_back(new_vm);
        SimOutput("Scheduler::NewTask(): Task " + to_string(task_id) + " placed on new VM on machine " + to_string(machine_id), 4);
        return;
    }

    // sara 3 wake up machine
    for (MachineId_t machine_id : machines) {
        MachineInfo_t machine_info = Machine_GetInfo(machine_id);
        if (machine_info.s_state == S0) continue;         
        if (machine_info.cpu != required_cpu) continue;
        if (machine_info.memory_size < required_memory + 8) continue;
 
        PendingTask pt;
        pt.vm_type = required_vm;
        pt.cpu_type = required_cpu;
        pt.task_id = task_id;
        pt.priority = priority;
        pending_tasks[machine_id] = pt;
 
        Machine_SetState(machine_id, S0);
        SimOutput("Scheduler::NewTask(): Waking machine " + to_string(machine_id) + " for task " + to_string(task_id), 3);
        return;
    }

    // sara uh oh no machine
    SimOutput("Scheduler::NewTask(): WARNING - no suitable machine found for task " + to_string(task_id), 0);

}

    // Turn on a machine, create a new VM, attach it to the VM, then add the task
    //
    // Turn on a machine, migrate an existing VM from a loaded machine....
    //
    // Other possibilities as desired
    // Priority_t priority = (task_id == 0 || task_id == 64)? HIGH_PRIORITY : MID_PRIORITY;
    // if(migrating) {
    //     VM_AddTask(vms[0], task_id, priority);
    // }
    // else {
    //     VM_(vms[task_id % active_machines], task_id, priority);
    // Skeleton code, you need to change it according to your algorithm

void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for (VMId_t vm : vms) {
        VMInfo_t info = VM_GetInfo(vm);
        if (info.active_tasks.empty()) {
            VM_Shutdown(vm);
        }
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
}

// Public interface below

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning(): SLA violation for task " + to_string(task_id) + " at time " + to_string(time), 1);
    
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
    // sara 
    SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " state change done at " + to_string(time), 3);
 
    auto ready = pending_tasks.find(machine_id);
    if (ready != pending_tasks.end()) {
        PendingTask& pt = ready->second;
        MachineInfo_t machine_info = Machine_GetInfo(machine_id);
 
        if (machine_info.s_state == S0) {
            VMId_t new_vm = VM_Create(pt.vm_type, pt.cpu_type);
            VM_Attach(new_vm, machine_id);
            VM_AddTask(new_vm, pt.task_id, pt.priority);
            Scheduler.AddVM(new_vm);
            SimOutput("StateChangeComplete(): Placed pending task " + to_string(pt.task_id) + " on machine " + to_string(machine_id), 3);
        }
        pending_tasks.erase(ready);
    }
}
