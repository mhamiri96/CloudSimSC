package org.cloudbus.cloudsim.examples;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.serverless.ServerlessWorkloadReader;
import org.cloudbus.cloudsim.serverless.FibonacciWorkflowGenerator;
import org.cloudbus.cloudsim.serverless.WorkflowBroker;
import org.cloudbus.cloudsim.serverless.WorkflowDAG;
import org.cloudbus.cloudsim.serverless.CustomVmAllocationPolicy;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

/**
 * Example of workflow scheduling with Fibonacci DAG
 */
public class WorkflowSchedulingExample {
    private static List<Cloudlet> cloudletList;
    private static List<Vm> vmlist;

    public static void main(String[] args) {
        System.out.println("Starting WorkflowSchedulingExample...");

        try {
            // Initialize CloudSim
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;

            CloudSim.init(num_user, calendar, trace_flag);

            // Create Datacenter
            Datacenter datacenter0 = createDatacenter("Datacenter_0");
            if (datacenter0 == null) {
                System.out.println("Failed to create datacenter. Exiting...");
                return;
            }

            // Generate Fibonacci workflow
            FibonacciWorkflowGenerator workflowGenerator = new FibonacciWorkflowGenerator(1000);
            int initialN = 5; // Use a fixed value for testing
            
            WorkflowDAG workflow = workflowGenerator.generateWorkflow(initialN);
            System.out.println("Generated Fibonacci workflow with initial n=" + initialN + 
                             " and " + workflow.getAllFunctions().size() + " functions");

            // Create Workflow Broker
            WorkflowBroker broker = new WorkflowBroker("Broker", workflow);
            int brokerId = broker.getId();

            // Create VMs
            vmlist = createVms(brokerId, 3); // Create only 3 VMs to match hosts
            
            // Submit VMs to broker
            broker.submitVmList(vmlist);
            
            // Submit cloudlets to broker
            List<Cloudlet> cloudlets = new ArrayList<>();
            for (org.cloudbus.cloudsim.serverless.WorkflowFunction function : workflow.getAllFunctions()) {
                cloudlets.add(function.getCloudlet());
            }

            for (int i = 0; i < cloudlets.size(); i++) {
    Cloudlet cloudlet = cloudlets.get(i);
    cloudlet.setUserId(brokerId);
    cloudlet.setVmId(i % vmlist.size());  // Distribute cloudlets across VMs
    System.out.println("Cloudlet " + cloudlet.getCloudletId() + 
                     " assigned to VM " + cloudlet.getVmId() +
                     " with user ID " + cloudlet.getUserId());
}

            broker.submitCloudletList(cloudlets);

            // Start simulation
            CloudSim.startSimulation();
            CloudSim.stopSimulation();


//             for (int i = 0; i < cloudletList.size(); i++) {
//                 Cloudlet cloudlet = cloudletList.get(i);
//                 cloudlet.setUserId(brokerId);
//                 cloudlet.setVmId(i % vmlist.size());  // Distribute cloudlets across VMs
//                 System.out.println("Cloudlet " + cloudlet.getCloudletId() + 
//                         " assigned to VM " + cloudlet.getVmId() +
//                         " with user ID " + cloudlet.getUserId());
// }
            // Print results
            List<Cloudlet> results = broker.getCloudletReceivedList();
            printWorkflowResults(results, workflow);

            System.out.println("WorkflowSchedulingExample finished!");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unwanted errors happened");
        }
    }

    private static Datacenter createDatacenter(String name) {
        // Create hosts with PEs
           List<Host> hostList = new ArrayList<>();
    
    for (int i = 0; i < 3; i++) {
        List<Pe> peList = new ArrayList<>();
        for (int j = 0; j < 4; j++) {
            peList.add(new Pe(j, new PeProvisionerSimple(1000)));
        }
        try {
            Host host = new Host(
                i,
                new RamProvisionerSimple(16384), // 16GB RAM
                new BwProvisionerSimple(10000),  // 10Gbps bandwidth
                1000000, // 1TB storage
                peList,
                new VmSchedulerTimeShared(peList)
            );
            hostList.add(host);
            System.out.println("Created host " + i + " with " + 
                             host.getRam() + "MB RAM, " +
                             host.getNumberOfPes() + " PEs, " +
                             host.getStorage() + "MB storage");
        } catch (Exception e) {
            System.err.println("Failed to create host " + i + ": " + e.getMessage());
        }
    } 
        if (hostList.isEmpty()) {
            System.err.println("No hosts were created successfully!");
            return null;
        }
        
        System.out.println("Successfully created " + hostList.size() + " hosts");
        
        // Create Datacenter characteristics
        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
            "x86", "Linux", "Xen", 
            hostList, 10.0, 3.0, 0.05, 0.001, 0.0
        );

        CustomVmAllocationPolicy allocationPolicy = new CustomVmAllocationPolicy(hostList);


        // Create Datacenter
        try {
            Datacenter datacenter = new Datacenter(
                name, 
                characteristics, 
                allocationPolicy, 
                new LinkedList<Storage>(), 
                0
            );
            System.out.println("Successfully created datacenter: " + name);
            return datacenter;
        } catch (Exception e) {
            System.err.println("Failed to create datacenter: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private static List<Vm> createVms(int brokerId, int count) {
        List<Vm> vms = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            Vm vm = new Vm(
                i, 
                brokerId, 
                1000, // MIPS
                1,    // Number of PEs
                1024, // 1GB RAM
                1000, // bandwidth
                10000, // storage
                "Xen", 
                new CloudletSchedulerTimeShared()
            );
            
            vms.add(vm);
            System.out.println("Created VM " + i + " with " + vm.getNumberOfPes() + " PEs and " + vm.getRam() + " MB RAM");
        }
        
        return vms;
    }

    private static void printWorkflowResults(List<Cloudlet> results, WorkflowDAG workflow) {
        if (results == null || results.isEmpty()) {
            System.out.println("No results to display");
            return;
        }
        
        DecimalFormat dft = new DecimalFormat("###.##");
        
        System.out.println("\n========== WORKFLOW RESULTS ==========");
        System.out.println("Cloudlet ID\tInput\tStatus\tVM ID\tTime\tStart Time\tFinish Time");
        
        for (Cloudlet cloudlet : results) {
            // Find the corresponding workflow function
            String inputValue = "N/A";
            for (org.cloudbus.cloudsim.serverless.WorkflowFunction function : workflow.getAllFunctions()) {
                if (function.getCloudlet() != null && 
                    function.getCloudlet().getCloudletId() == cloudlet.getCloudletId()) {
                    inputValue = String.valueOf(function.getInputValue());
                    break;
                }
            }
            
            System.out.println(
                cloudlet.getCloudletId() + "\t\t" +
                inputValue + "\t" +
                (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS ? "SUCCESS" : "FAILED") + "\t" +
                cloudlet.getVmId() + "\t" +
                dft.format(cloudlet.getActualCPUTime()) + "\t" +
                dft.format(cloudlet.getExecStartTime()) + "\t\t" +
                dft.format(cloudlet.getFinishTime())
            );
        }
        
        // Calculate makespan (total workflow execution time)
        double makespan = 0;
        for (Cloudlet cloudlet : results) {
            if (cloudlet.getFinishTime() > makespan) {
                makespan = cloudlet.getFinishTime();
            }
        }
        
        System.out.println("\nWorkflow Makespan: " + dft.format(makespan) + " seconds");
        System.out.println("Total Cloudlets: " + results.size());
    }
}