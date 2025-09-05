/*
 * Serverless function data model
 */
package org.cloudbus.cloudsim.serverless;

public class ServerlessFunction {
    private final int functionId;
    private final String hashOwner;
    private final String hashApp;
    private final String hashFunction;
    private final String trigger;
    private final int[] invocationsPerMinute;
    
    public ServerlessFunction(int functionId, String hashOwner, String hashApp, 
                           String hashFunction, String trigger, int[] invocationsPerMinute) {
        this.functionId = functionId;
        this.hashOwner = hashOwner;
        this.hashApp = hashApp;
        this.hashFunction = hashFunction;
        this.trigger = trigger;
        this.invocationsPerMinute = invocationsPerMinute;
    }
    
    // Getters
    public int getFunctionId() { return functionId; }
    public String getHashOwner() { return hashOwner; }
    public String getHashApp() { return hashApp; }
    public String getHashFunction() { return hashFunction; }
    public String getTrigger() { return trigger; }
    public int[] getInvocationsPerMinute() { return invocationsPerMinute; }
    
    public int getTotalInvocations() {
        int total = 0;
        for (int inv : invocationsPerMinute) {
            total += inv;
        }
        return total;
    }
}