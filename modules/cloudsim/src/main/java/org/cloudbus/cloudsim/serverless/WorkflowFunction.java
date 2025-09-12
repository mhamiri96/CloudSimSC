package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Cloudlet;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a function in a workflow DAG
 */
public class WorkflowFunction {
    private int functionId;
    private String functionName;
    private int inputValue;
    private List<WorkflowFunction> children;
    private Cloudlet cloudlet;
    private boolean completed;
    
    public WorkflowFunction(int functionId, String functionName, int inputValue) {
        this.functionId = functionId;
        this.functionName = functionName;
        this.inputValue = inputValue;
        this.children = new ArrayList<>();
        this.completed = false;
    }
    
    // Getters and setters
    public int getFunctionId() { return functionId; }
    public String getFunctionName() { return functionName; }
    public int getInputValue() { return inputValue; }
    public List<WorkflowFunction> getChildren() { return children; }
    public Cloudlet getCloudlet() { return cloudlet; }
    public void setCloudlet(Cloudlet cloudlet) { this.cloudlet = cloudlet; }
    public boolean isCompleted() { return completed; }
    public void setCompleted(boolean completed) { this.completed = completed; }
    
    public void addChild(WorkflowFunction child) {
        children.add(child);
    }
}