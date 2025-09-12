package org.cloudbus.cloudsim.serverless;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a workflow DAG
 */
public class WorkflowDAG {
    private WorkflowFunction root;
    private List<WorkflowFunction> allFunctions;
    
    public WorkflowDAG() {
        this.allFunctions = new ArrayList<>();
    }
    
    public WorkflowFunction getRoot() { return root; }
    public void setRoot(WorkflowFunction root) { 
        this.root = root;
        allFunctions.add(root);
    }
    
    public List<WorkflowFunction> getAllFunctions() { return allFunctions; }
    
    public void addFunction(WorkflowFunction function) {
        allFunctions.add(function);
    }
}