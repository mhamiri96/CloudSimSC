/*
 * Main simulation class for serverless computing
 */
package org.cloudbus.cloudsim.serverless;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Calendar;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

public class ServerlessSimulation {
    
    public static void main(String[] args) {
        try {
            // Initialize CloudSim
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;
            
            CloudSim.init(num_user, calendar, trace_flag);
            
            // Create datacenter
            ServerlessDatacenter datacenter = createDatacenter();
            
            // Create broker
            DatacenterBroker broker = createBroker();
            
            // Create VMs
            List<Vm> vms = createVms(broker);
            
            // Create custom workload reader
            ServerlessWorkloadReader workloadReader = new ServerlessWorkloadReader(
                "invocations_per_function_md.anon.d01.csv", 
                1000 // PE rating
            );
            
            // Generate cloudlets from workload
            List<Cloudlet> cloudlets = workloadReader.generateCloudletsFromWorkload();
            
            // Create serverless scheduler
            ServerlessScheduler scheduler = new ServerlessScheduler(vms);
            
            // Get serverless functions for scheduling decisions
            List<ServerlessFunction> functions = workloadReader.generateServerlessWorkload();
            
            // Schedule cloudlets using custom scheduler
            for (int i = 0; i < cloudlets.size(); i++) {
                Cloudlet cloudlet = cloudlets.get(i);
                ServerlessFunction function = functions.get(i / 1440); // Approximate mapping
                scheduler.scheduleFunction(cloudlet, function);
            }
            
            // Submit cloudlets to broker
            broker.submitCloudletList(cloudlets);
            
            // Start simulation
            CloudSim.startSimulation();
            
            // Get results
            List<Cloudlet> receivedCloudlets = broker.getCloudletReceivedList();
            CloudSim.stopSimulation();
            
            // Print results
            printResults(receivedCloudlets, scheduler);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static ServerlessDatacenter createDatacenter() {
        // Implement datacenter creation logic
        // This should create a datacenter with serverless-specific configurations
        return new ServerlessDatacenter();
    }
    
    private static DatacenterBroker createBroker() {
        // Create broker
        DatacenterBroker broker = null;
        try {
            broker = new DatacenterBroker("ServerlessBroker");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return broker;
    }
    
    private static List<Vm> createVms(DatacenterBroker broker) {
        List<Vm> vms = new ArrayList<>();
        
        // Create VMs with serverless-specific configurations
        for (int i = 0; i < 5; i++) {
            Vm vm = new Vm(i, broker.getId(), 1000, 1, 1024, 1000, 1000, "Xen", new CloudletSchedulerTimeShared());
            vms.add(vm);
        }
        
        broker.submitVmList(vms);
        return vms;
    }
    
    private static void printResults(List<Cloudlet> cloudlets, ServerlessScheduler scheduler) {
        System.out.println("=== Serverless Simulation Results ===");
        System.out.println("Total Cloudlets processed: " + cloudlets.size());
        
        // Print VM load statistics
        System.out.println("\nVM Load Statistics:");
        for (Map.Entry<Vm, Integer> entry : scheduler.getVmLoadStats().entrySet()) {
            System.out.println("VM " + entry.getKey().getId() + ": " + entry.getValue() + " cloudlets");
        }
        
        // Print cloudlet completion times
        System.out.println("\nCloudlet Completion Times:");
        for (Cloudlet cloudlet : cloudlets) {
            System.out.println("Cloudlet " + cloudlet.getCloudletId() + 
                             ": " + cloudlet.getFinishTime() + " seconds");
        }
    }
}