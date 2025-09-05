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
    private ArrayList<Cloudlet> cloudlets;

    public ServerlessWorkloadReader(String fileName, int rating) throws FileNotFoundException {
        this.file = new File(fileName);
        if (!file.exists()) {
            throw new FileNotFoundException("File not found: " + fileName);
        }
        this.rating = rating;
    }

    @Override
    public ArrayList<Cloudlet> generateWorkload() {
        if (cloudlets == null) {
            cloudlets = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                reader.readLine(); // Skip header
                while ((line = reader.readLine()) != null) {
                    processLine(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return cloudlets;
    }

    private void processLine(String line) {
        String[] parts = line.split(",");
        if (parts.length < 1444) return; // Ensure enough columns

        String hashFunction = parts[2].trim();
        for (int minute = 0; minute < 1440; minute++) {
            int invocations = Integer.parseInt(parts[4 + minute].trim());
            for (int i = 0; i < invocations; i++) {
                createCloudlet(hashFunction, minute);
            }
        }
    }

    private void createCloudlet(String hashFunction, int minute) {
        long submitTime = minute * 60; // Convert minute to seconds
        // Adjust these values based on your function characteristics
        int runTime = 10; // Example: 10 seconds runtime
        int numProc = 1;  // Single-threaded function

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
        cloudlet.setSubmissionTime(submitTime);
        cloudlets.add(cloudlet);
    }
}