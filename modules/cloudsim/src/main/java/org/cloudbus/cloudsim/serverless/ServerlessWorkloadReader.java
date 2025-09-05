package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.util.WorkloadModel;

import java.io.*;
import java.util.*;

public class ServerlessWorkloadReader implements WorkloadModel {
    private final File file;
    private final int rating;
    private final int maxInvocationsPerMinute;
    private final int maxRecordsToProcess;
    private ArrayList<Cloudlet> cloudlets;
    private boolean traceProcessed = false;

    public ServerlessWorkloadReader(String fileName, int rating) throws FileNotFoundException {
        this(fileName, rating, 1000, 10000); // Default cap of 1000 invocations per minute, 10k records
    }

    public ServerlessWorkloadReader(String fileName, int rating, int maxInvocationsPerMinute, int maxRecordsToProcess) 
            throws FileNotFoundException {
        this.file = new File(fileName);
        if (!file.exists()) {
            throw new FileNotFoundException("File not found: " + fileName);
        }
        this.rating = rating;
        this.maxInvocationsPerMinute = maxInvocationsPerMinute;
        this.maxRecordsToProcess = maxRecordsToProcess;
    }

    @Override
    public ArrayList<Cloudlet> generateWorkload() {
        if (cloudlets == null) {
            cloudlets = new ArrayList<>();
        }
        
        if (!traceProcessed) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                reader.readLine(); // Skip header
                
                int lineCount = 0;
                while ((line = reader.readLine()) != null && lineCount < maxRecordsToProcess) {
                    processLine(line, ++lineCount);
                    
                    // Process in batches to avoid memory overload
                    if (lineCount % 1000 == 0) {
                        System.gc(); // Suggest garbage collection
                        logMemoryUsage("Processed " + lineCount + " records");
                    }
                }
                traceProcessed = true;
                logMemoryUsage("Finished processing " + lineCount + " records");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        return cloudlets;
    }

    private void processLine(String line, int lineNumber) {
        String[] parts = line.split(",");
        if (parts.length < 1444) {
            System.err.println("Skipping line " + lineNumber + ": insufficient columns");
            return;
        }

        String hashFunction = parts[2].trim();
        for (int minute = 0; minute < 1440; minute++) {
            try {
                int invocations = Integer.parseInt(parts[4 + minute].trim());
                
                // Apply cap to prevent memory overload
                if (invocations > maxInvocationsPerMinute) {
                    invocations = maxInvocationsPerMinute;
                }
                
                for (int i = 0; i < invocations; i++) {
                    createCloudlet(hashFunction, minute, lineNumber, minute);
                }
            } catch (NumberFormatException e) {
                System.err.println("Invalid number format in line " + lineNumber + 
                                 ", minute " + minute + ": " + parts[4 + minute]);
            }
        }
    }

    private void createCloudlet(String hashFunction, int minute, int lineNumber, int minuteIndex) {
        long submitTime = minute * 60; // Convert minute to seconds
        
        // Use function hash to determine resource requirements consistently
        int functionId = Math.abs(hashFunction.hashCode());
        int runTime = 10 + (functionId % 91); // 10-100 seconds runtime
        int memory = 128 + (functionId % 385); // 128-512 MB memory
        int numProc = 1; // Single-threaded function

        long length = runTime * rating;
        UtilizationModel utilizationModel = new UtilizationModelFull();
        
        Cloudlet cloudlet = new Cloudlet(
            cloudlets.size() + 1,
            length,
            numProc,
            0, 0,
            utilizationModel,
            utilizationModel,
            utilizationModel
        );
        
        // Set submission time and user-defined properties
        cloudlet.setSubmissionTime(submitTime);
        cloudlet.setUserId(functionId % 1000); // Simulate user ID
        cloudlet.setVmId(-1); // To be assigned later
        
        cloudlets.add(cloudlet);
    }

    // Helper method to log memory usage
    private void logMemoryUsage(String context) {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        long maxMemory = runtime.maxMemory() / (1024 * 1024);
        long freeMemory = runtime.freeMemory() / (1024 * 1024);
        
        System.out.println(context + " - Memory usage: " + 
                          "Used=" + usedMemory + "MB, " +
                          "Free=" + freeMemory + "MB, " +
                          "Max=" + maxMemory + "MB");
    }

    public int getMaxInvocationsPerMinute() {
        return maxInvocationsPerMinute;
    }
    
    public int getMaxRecordsToProcess() {
        return maxRecordsToProcess;
    }
    
    public int getTotalCloudlets() {
        return cloudlets != null ? cloudlets.size() : 0;
    }
    
    public void clearCloudlets() {
        if (cloudlets != null) {
            cloudlets.clear();
            System.gc();
        }
        traceProcessed = false;
    }
}