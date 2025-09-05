package org.cloudbus.cloudsim.examples;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.serverless.ServerlessWorkloadReader;;

/**
 * A modified example showing how to create a data center and run serverless workloads
 * based on the invocations_per_function_md.anon.d01.csv trace file.
 */
public class ServerlessWorkloadExample {
    /** The cloudlet list. */
    private static List<Cloudlet> cloudletList;
    /** The vmlist. */
    private static List<Vm> vmlist;

    /**
     * Creates main() to run this example.
     *
     * @param args the args
     */
    @SuppressWarnings("unused")
    public static void main(String[] args) {
        Log.printLine("Starting ServerlessWorkloadExample...");

        try {
            // First step: Initialize the CloudSim package
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;

            CloudSim.init(num_user, calendar, trace_flag);

            // Second step: Create Datacenter
            Datacenter datacenter0 = createDatacenter("Datacenter_0");

            // Third step: Create Broker
            DatacenterBroker broker = createBroker();
            int brokerId = broker.getId();

            // Fourth step: Create multiple virtual machines to handle serverless workload
            vmlist = new ArrayList<Vm>();

            // Create 10 VMs to handle the serverless workload
            int numVms = 10;
            for (int i = 0; i < numVms; i++) {
                // VM description
                int vmid = i;
                int mips = 1000; // MIPS rating
                long size = 10000; // image size (MB)
                int ram = 2048; // vm memory (MB) - increased for serverless workloads
                long bw = 1000;
                int pesNumber = 4; // number of cpus - increased for parallel execution
                String vmm = "Xen"; // VMM name

                // create VM
                Vm vm = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
                vmlist.add(vm);
            }

            // submit vm list to the broker
            broker.submitVmList(vmlist);

            // Fifth step: Create Cloudlets from serverless workload trace
            cloudletList = new ArrayList<Cloudlet>();
            
            // Use the ServerlessWorkloadReader to generate cloudlets from the trace file
            ServerlessWorkloadReader workloadReader = new ServerlessWorkloadReader(
                "/home/mha/serverless/azurefunctions-dataset2019/invocations_per_function_md.anon.d01.csv", 
                1000 // MIPS rating
            );
            
            cloudletList = workloadReader.generateWorkload();
            
            // Set VM IDs for the cloudlets (round-robin assignment to VMs)
            for (int i = 0; i < cloudletList.size(); i++) {
                Cloudlet cloudlet = cloudletList.get(i);
                cloudlet.setUserId(brokerId);
                cloudlet.setVmId(i % numVms); // Distribute cloudlets across VMs
            }

            // submit cloudlet list to the broker
            broker.submitCloudletList(cloudletList);

            // Sixth step: Starts the simulation
            CloudSim.startSimulation();

            CloudSim.stopSimulation();

            // Final step: Print results when simulation is over
            List<Cloudlet> newList = broker.getCloudletReceivedList();
            printCloudletList(newList);

            // Print summary statistics
            printSimulationSummary(newList);

            Log.printLine("ServerlessWorkloadExample finished!");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }

    /**
     * Creates the datacenter with multiple hosts to handle serverless workload.
     *
     * @param name the name
     *
     * @return the datacenter
     */
    private static Datacenter createDatacenter(String name) {
        // Create a list to store hosts
        List<Host> hostList = new ArrayList<Host>();

        // Create 5 hosts for the datacenter
        int numHosts = 5;
        for (int i = 0; i < numHosts; i++) {
            // Each host has 4 PEs
            List<Pe> peList = new ArrayList<Pe>();
            int mips = 1000;

            for (int j = 0; j < 4; j++) {
                peList.add(new Pe(j, new PeProvisionerSimple(mips)));
            }

            int hostId = i;
            int ram = 16384; // 16 GB host memory (MB)
            long storage = 1000000; // 1 TB host storage
            int bw = 10000; // 10 Gbps bandwidth

            hostList.add(
                new Host(
                    hostId,
                    new RamProvisionerSimple(ram),
                    new BwProvisionerSimple(bw),
                    storage,
                    peList,
                    new VmSchedulerTimeShared(peList)
                )
            );
        }

        // Create DatacenterCharacteristics
        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0;
        double cost = 3.0;
        double costPerMem = 0.05;
        double costPerStorage = 0.001;
        double costPerBw = 0.0;
        LinkedList<Storage> storageList = new LinkedList<Storage>();

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem,
                costPerStorage, costPerBw);

        // Create the Datacenter
        Datacenter datacenter = null;
        try {
            datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
    }

    /**
     * Creates the broker.
     *
     * @return the datacenter broker
     */
    private static DatacenterBroker createBroker() {
        DatacenterBroker broker = null;
        try {
            broker = new DatacenterBroker("Broker");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return broker;
    }

    /**
     * Prints the Cloudlet objects.
     *
     * @param list list of Cloudlets
     */
    private static void printCloudletList(List<Cloudlet> list) {
        int size = list.size();
        Cloudlet cloudlet;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + "Time" + indent
                + "Start Time" + indent + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < Math.min(size, 100); i++) { // Only print first 100 cloudlets
            cloudlet = list.get(i);
            Log.print(indent + cloudlet.getCloudletId() + indent + indent);

            if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");

                Log.printLine(indent + indent + cloudlet.getResourceId()
                        + indent + indent + indent + cloudlet.getVmId()
                        + indent + indent
                        + dft.format(cloudlet.getActualCPUTime()) + indent
                        + indent + dft.format(cloudlet.getExecStartTime())
                        + indent + indent
                        + dft.format(cloudlet.getFinishTime()));
            }
        }
        
        if (size > 100) {
            Log.printLine("... and " + (size - 100) + " more cloudlets");
        }
    }
    
    /**
     * Prints simulation summary statistics.
     *
     * @param list list of Cloudlets
     */
    private static void printSimulationSummary(List<Cloudlet> list) {
        int size = list.size();
        if (size == 0) return;
        
        double totalTime = 0;
        int completed = 0;
        double minTime = Double.MAX_VALUE;
        double maxTime = 0;
        
        for (Cloudlet cloudlet : list) {
            if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
                completed++;
                double time = cloudlet.getActualCPUTime();
                totalTime += time;
                minTime = Math.min(minTime, time);
                maxTime = Math.max(maxTime, time);
            }
        }
        
        double avgTime = totalTime / completed;
        
        DecimalFormat dft = new DecimalFormat("###.##");
        Log.printLine();
        Log.printLine("========== SIMULATION SUMMARY ==========");
        Log.printLine("Total Cloudlets: " + size);
        Log.printLine("Completed Cloudlets: " + completed);
        Log.printLine("Success Rate: " + dft.format((double) completed / size * 100) + "%");
        Log.printLine("Average Execution Time: " + dft.format(avgTime) + " seconds");
        Log.printLine("Minimum Execution Time: " + dft.format(minTime) + " seconds");
        Log.printLine("Maximum Execution Time: " + dft.format(maxTime) + " seconds");
        Log.printLine("Total Simulation Time: " + dft.format(CloudSim.clock()) + " seconds");
    }
}