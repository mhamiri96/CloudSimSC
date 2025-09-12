package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import java.util.List;

/**
 * Custom VM allocation policy that ensures proper host-VM binding
 */
public class CustomVmAllocationPolicy extends VmAllocationPolicySimple {
    
    public CustomVmAllocationPolicy(List<? extends Host> hostList) {
        super(hostList);
    }
    
    @Override
    public boolean allocateHostForVm(Vm vm) {
        // Add debug logging
        System.out.println("Allocating host for VM " + vm.getId() + " with " + 
                         vm.getMips() + " MIPS, " + vm.getRam() + " MB RAM");
        
        boolean result = super.allocateHostForVm(vm);
        
        if (result) {
            Host host = getHost(vm);
            System.out.println("VM " + vm.getId() + " allocated to Host " + 
                             (host != null ? host.getId() : "NULL"));
        } else {
            System.err.println("Failed to allocate host for VM " + vm.getId());
        }
        
        return result;
    }
    
    @Override
    public Host getHost(Vm vm) {
        Host host = super.getHost(vm);
        if (host == null) {
            System.err.println("Warning: No host found for VM " + vm.getId());
        }
        return host;
    }
    
    @Override
    public Host getHost(int vmId, int userId) {
        Host host = super.getHost(vmId, userId);
        if (host == null) {
            System.err.println("Warning: No host found for VM " + vmId + ", user " + userId);
        }
        return host;
    }
    
    // Additional helper methods for your custom policy
    public void printAllocationStatus() {
        System.out.println("=== Current VM Allocation Status ===");
        for (Host host : getHostList()) {
            System.out.println("Host " + host.getId() + " has " + 
                             host.getVmList().size() + " VMs");
            for (Vm vm : host.getVmList()) {
                System.out.println("  - VM " + vm.getId() + " (User: " + vm.getUserId() + ")");
            }
        }
    }
}