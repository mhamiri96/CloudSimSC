package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.serverless.CustomVmAllocationPolicy;

import java.util.*;

/**
 * A broker that schedules workflow functions using FIFO algorithm
 */
public class WorkflowBroker extends DatacenterBroker {
    private WorkflowDAG workflow;
    private Queue<WorkflowFunction> readyQueue;
    private Map<Integer, WorkflowFunction> functionMap;
    private Map<Integer, Integer> pendingDependencies;
    private boolean vmsCreated = false;
    
    public WorkflowBroker(String name, WorkflowDAG workflow) throws Exception {
        super(name);
        this.workflow = workflow;
        this.readyQueue = new LinkedList<>();
        this.functionMap = new HashMap<>();
        this.pendingDependencies = new HashMap<>();
        
        initializeWorkflow();
    }
    
    private void initializeWorkflow() {
        // Map functions by ID and count dependencies
        for (WorkflowFunction function : workflow.getAllFunctions()) {
            functionMap.put(function.getFunctionId(), function);
            pendingDependencies.put(function.getFunctionId(), 0);
        }
        
        // For each function, increment dependency count for its children
        for (WorkflowFunction function : workflow.getAllFunctions()) {
            for (WorkflowFunction child : function.getChildren()) {
                int currentDependencies = pendingDependencies.get(child.getFunctionId());
                int newDependencies = currentDependencies + 1;
                pendingDependencies.put(child.getFunctionId(), newDependencies);
            }
        }
        
        // Add functions with no dependencies to the ready queue
        for (WorkflowFunction function : workflow.getAllFunctions()) {
            if (pendingDependencies.get(function.getFunctionId()) == 0) {
                readyQueue.add(function);
            }
        }
    }
    
    @Override
    public void startEntity() {
        super.startEntity();
        // Don't schedule immediately - wait for VMs to be created
    }
    
 
    
    private void handleCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        WorkflowFunction completedFunction = null;
        
        // Find the function that completed
        for (WorkflowFunction function : workflow.getAllFunctions()) {
            if (function.getCloudlet() != null && 
                function.getCloudlet().getCloudletId() == cloudlet.getCloudletId()) {
                completedFunction = function;
                break;
            }
        }
        
        if (completedFunction != null) {
            completedFunction.setCompleted(true);
            
            // Decrement dependency count for children and add to ready queue if no more dependencies
            for (WorkflowFunction child : completedFunction.getChildren()) {
                int currentDependencies = pendingDependencies.get(child.getFunctionId());
                int newDependencies = currentDependencies - 1;
                pendingDependencies.put(child.getFunctionId(), newDependencies);
                
                if (newDependencies == 0) {
                    readyQueue.add(child);
                }
            }
            
            // Schedule the next function if VMs are ready
            if (vmsCreated) {
                scheduleNextFunction();
            }
        }
    }
    
    
//    submitCloudletList(Arrays.asList(cloudlet));


        @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case CloudSimTags.VM_CREATE_ACK:
                // Let the parent handle VM creation first
                super.processEvent(ev);
                
            // Check if all VMs have been created and allocated
            if (getVmsCreatedList().size() == getVmList().size()) {
                System.out.println("All VMs created and allocated, starting cloudlet scheduling");
                
                // Verify all VMs are properly allocated to hosts
                for (Vm vm : getVmsCreatedList()) {
                    Host host = getVmAllocationPolicy().getHost(vm.getId(), getId());
                    if (host == null) {
                        System.err.println("Error: VM " + vm.getId() + " not allocated to any host!");
                    } else {
                        System.out.println("VM " + vm.getId() + " is on Host " + host.getId());
                    }
                }
                
                scheduleAllReadyFunctions();
            }
                break;
                
            case CloudSimTags.CLOUDLET_RETURN:
                handleCloudletReturn(ev);
                break;
                
            default:
                super.processEvent(ev);
        }
    }
    
    private void scheduleAllReadyFunctions() {
        System.out.println("Scheduling " + readyQueue.size() + " ready functions");
        while (!readyQueue.isEmpty()) {
            scheduleNextFunction();
        }
    }
    
    // private void scheduleNextFunction() {
    //     if (!readyQueue.isEmpty()) {
    //         WorkflowFunction nextFunction = readyQueue.poll();
    //         Cloudlet cloudlet = nextFunction.getCloudlet();
            
    //         if (cloudlet != null) {
    //             // Set the correct user ID (broker ID) for the cloudlet
    //             cloudlet.setUserId(getId());  // This is the critical fix
                
    //             // Verify VM exists before submitting cloudlet
    //             if (cloudlet.getVmId() >= 0) {
    //                 submitCloudlet(cloudlet);
    //             } else {
    //                 System.err.println("Warning: No VM assigned for cloudlet " + cloudlet.getCloudletId());
    //             }
    //         }
    //     }
    // }

    public void submitCloudlet(Cloudlet cloudlet) {
        if (getCloudletList() == null) {
            setCloudletList(new ArrayList<Cloudlet>());
        }
        getCloudletList().add(cloudlet);
        
        // Get the first datacenter ID using CloudSim's entity system
        int datacenterId = CloudSim.getCloudResourceList().get(0);
        sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
    }
    
    // Alternative method to get datacenter IDs
    private List<Integer> getDatacenterIds() {
        return CloudSim.getCloudResourceList();
    }

     public VmAllocationPolicy getVmAllocationPolicy() {
        if (getDatacenterIds() == null || getDatacenterIds().isEmpty()) {
            System.err.println("No datacenters available");
            return null;
        }
        
        int datacenterId = getDatacenterIds().get(0);
        Datacenter datacenter = (Datacenter) CloudSim.getEntity(datacenterId);
        
        if (datacenter != null) {
            return datacenter.getVmAllocationPolicy();
        } else {
            System.err.println("Datacenter not found with ID: " + datacenterId);
            return null;
        }
    }

    public void verifyVmAllocation(int vmId) {
    VmAllocationPolicy policy = getVmAllocationPolicy();
    if (policy != null) {
        Host host = policy.getHost(vmId, getId());
        if (host != null) {
            System.out.println("VM " + vmId + " is allocated to Host " + host.getId());
        } else {
            System.err.println("VM " + vmId + " is not allocated to any host");
        }
    }
}

// Use this in your scheduling method
private void scheduleNextFunction() {
    if (!readyQueue.isEmpty()) {
        WorkflowFunction nextFunction = readyQueue.poll();
        Cloudlet cloudlet = nextFunction.getCloudlet();
        
        if (cloudlet != null) {
            // Verify VM allocation before submitting cloudlet
            verifyVmAllocation(cloudlet.getVmId());
            
            // Set the correct user ID (broker ID) for the cloudlet
            cloudlet.setUserId(getId());
            
            submitCloudlet(cloudlet);
        }
    }
}
}