package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.serverless.ServerlessWorkloadReader;

import java.io.FileNotFoundException;
import java.util.Random;

/**
 * Generates a Fibonacci-like workflow DAG
 */
public class FibonacciWorkflowGenerator {
    private int rating;
    private ServerlessWorkloadReader workloadReader;
    private Random random;
    
    public FibonacciWorkflowGenerator(int rating) {
        this.rating = rating;
        this.random = new Random();
    }
    
    public WorkflowDAG generateWorkflow(int initialN) throws FileNotFoundException {
        WorkflowDAG workflow = new WorkflowDAG();
        
        // Create root function with a random n < 10 (from the trace)
        WorkflowFunction root = new WorkflowFunction(0, "fib", initialN);
        workflow.setRoot(root);
        
        // Recursively generate the Fibonacci DAG
        generateFibonacciFunctions(root, workflow);
        
        // Create cloudlets for all functions
        createCloudletsForWorkflow(workflow);
        
        return workflow;
    }
    
    private void generateFibonacciFunctions(WorkflowFunction parent, WorkflowDAG workflow) {
        int n = parent.getInputValue();
        
        if (n <= 1) {
            // Base case - no children
            return;
        }
        
        // Create children for n-1 and n-2
        WorkflowFunction child1 = new WorkflowFunction(
            workflow.getAllFunctions().size(), 
            "fib", 
            n - 1
        );
        
        WorkflowFunction child2 = new WorkflowFunction(
            workflow.getAllFunctions().size(), 
            "fib", 
            n - 2
        );
        
        parent.addChild(child1);
        parent.addChild(child2);
        
        workflow.addFunction(child1);
        workflow.addFunction(child2);
        
        // Recursively generate children
        generateFibonacciFunctions(child1, workflow);
        generateFibonacciFunctions(child2, workflow);
    }
    
    private void createCloudletsForWorkflow(WorkflowDAG workflow) {
        for (WorkflowFunction function : workflow.getAllFunctions()) {
            int n = function.getInputValue();
            
            // Calculate execution time based on n (fibonacci complexity is exponential,
            // but we'll use a linear approximation for simulation purposes)
            int runTime = (int) Math.pow(2, n) * 10; // Exponential time complexity
            int numProc = 1;
            
            long length = runTime * rating;
            UtilizationModel utilizationModel = new UtilizationModelFull();
            
            Cloudlet cloudlet = new Cloudlet(
                function.getFunctionId(),
                length,
                numProc,
                0, 0,
                utilizationModel,
                utilizationModel,
                utilizationModel
            );
            
            
            function.setCloudlet(cloudlet);
        }
    }
    
    public int getInitialNFromTrace(String traceFile) throws FileNotFoundException {
        // Use the workload trace to get a random initial value for n
        // For simplicity, we'll just return a random value between 1 and 10
        return random.nextInt(10) + 1;
    }
}