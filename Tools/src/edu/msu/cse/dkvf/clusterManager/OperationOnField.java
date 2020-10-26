package edu.msu.cse.dkvf.clusterManager;


public class OperationOnField {
	public enum AggregationOperation {
		SUM, AVG
	}
	
	AggregationOperation operation;
	String field;
	public OperationOnField(AggregationOperation operation, String field){
		this.operation = operation;
		this.field = field;
	}
}