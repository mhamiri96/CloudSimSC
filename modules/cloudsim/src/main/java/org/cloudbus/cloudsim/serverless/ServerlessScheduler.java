/*
 * Custom serverless scheduling algorithm
 */
package org.cloudbus.cloudsim.serverless;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;

public class ServerlessScheduler {
    
    private final List<Vm> vms;
    private final Map<String, ServerlessFunction> functionCache;
    private final Map<Vm, Integer> vmLoad;
    
    public ServerlessScheduler(List<Vm> vms) {
        this.vms = vms;
        this.functionCache = new HashMap<>();
        this.vmLoad = new HashMap<>();
        
        // Initialize VM load tracking
        for (Vm vm : vms) {
            vmLoad.put(vm, 0);
        }
    }
    
    public void scheduleFunction(Cloudlet cloudlet, ServerlessFunction function) {
        // Implement your scheduling logic here
        
        // Example: Load balancing based on current VM load
        Vm selectedVm = selectVmForScheduling(cloudlet, function);
        
        if (selectedVm != null) {
            // Update VM load
            vmLoad.put(selectedVm, vmLoad.get(selectedVm) + 1);
            
            // Submit cloudlet to selected VM
            cloudlet.setVmId(selectedVm.getId());
            
            // Add to function cache for future reference
            String functionKey = function.getHashApp() + "_" + function.getHashFunction();
            functionCache.put(functionKey, function);
        }
    }
    
    private Vm selectVmForScheduling(Cloudlet cloudlet, ServerlessFunction function) {
        // Implement your scheduling policy
        
        // Example 1: Round-robin scheduling
        // return vms.get(cloudlet.getCloudletId() % vms.size());
        
        // Example 2: Load balancing (select VM with least load)
        Vm selectedVm = null;
        int minLoad = Integer.MAX_VALUE;
        
        for (Vm vm : vms) {
            int currentLoad = vmLoad.getOrDefault(vm, 0);
            if (currentLoad < minLoad) {
                minLoad = currentLoad;
                selectedVm = vm;
            }
        }
        
        // Example 3: Trigger-based scheduling
        // if (function.getTrigger().equals("http")) {
        //     // Schedule HTTP-triggered functions to high-performance VMs
        //     return selectHighPerformanceVm();
        // }
        
        return selectedVm;
    }
    
    public void releaseVmResources(Vm vm) {
        // Update VM load when a cloudlet completes
        int currentLoad = vmLoad.getOrDefault(vm, 0);
        vmLoad.put(vm, Math.max(0, currentLoad - 1));
    }
    
    public Map<Vm, Integer> getVmLoadStats() {
        return new HashMap<>(vmLoad);
    }
}