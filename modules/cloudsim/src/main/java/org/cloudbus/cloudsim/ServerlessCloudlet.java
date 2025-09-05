package org.cloudbus.cloudsim;

// Create a custom Cloudlet class instead of using addRequiredProperty
public class ServerlessCloudlet extends Cloudlet {
    private String hashFunction;
    private int minute;
    private int memory;
    
    public ServerlessCloudlet(int cloudletId, long cloudletLength, int pesNumber, 
                             long cloudletFileSize, long cloudletOutputSize,
                             UtilizationModel utilizationModelCpu,
                             UtilizationModel utilizationModelRam,
                             UtilizationModel utilizationModelBw,
                             String hashFunction, int minute, int memory) {
        super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, 
              cloudletOutputSize, utilizationModelCpu, utilizationModelRam, 
              utilizationModelBw);
        this.hashFunction = hashFunction;
        this.minute = minute;
        this.memory = memory;
    }
    
    // Add getters for your properties
    public String getHashFunction() { return hashFunction; }
    public int getMinute() { return minute; }
    public int getMemory() { return memory; }
}