package edu.msu.cse.dkvf.clusterManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.msu.cse.dkvf.clusterManager.OperationOnField.AggregationOperation;

public class ResultsReader {

	final static String SEPARATOR = "_";
	HashMap<String, ResultAggregator> varinationAggregatorsMap = new HashMap<>();
	
	// Extension can be for example _t.result
	public void readDir(String resultsDir, ArrayList<String> measurementsToRead, String extension) throws FileNotFoundException {
		if (resultsDir == null || measurementsToRead == null || extension == null)
			throw new IllegalArgumentException();

		File dir = new File(resultsDir);
		HashMap<String, ArrayList<File>> variationFileList = new HashMap<>();

		for (final File fileEntry : dir.listFiles()) {
			if (fileEntry.isFile() && fileEntry.getName().endsWith(extension)) {
				String[] fileEntryParts = fileEntry.getName().split(SEPARATOR);
				String variationName = fileEntryParts[2];

				if (!variationFileList.containsKey(variationName)) {
					variationFileList.put(variationName, new ArrayList<>());
				}
				variationFileList.get(variationName).add(fileEntry);
			}
		}

		for (Map.Entry<String, ArrayList<File>> v : variationFileList.entrySet()) {
			ResultAggregator ra = new ResultAggregator();
			ra.readResults(v.getValue(), measurementsToRead);
			varinationAggregatorsMap.put(v.getKey(), ra);
			
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();

		for (Map.Entry<String, ResultAggregator> v : varinationAggregatorsMap.entrySet()) {
			sb.append("Variation: ").append(v.getKey()).append("\n");
			sb.append(v.getValue().toString());
		}
		return sb.toString();
	}
	
	public Table readVarReuslts (String variationName, ArrayList<String> fieldsToRead){
		if (!varinationAggregatorsMap.containsKey(variationName))
			throw new IllegalArgumentException("No such variation name: " + variationName);
		return varinationAggregatorsMap.get(variationName).readFields(fieldsToRead);
	}
	
	public Table readAggResults(OperationOnField[] opFields) {
		Table result = new Table();
		for (OperationOnField of : opFields) {
			result.topHeaders.add(of.operation + "." + of.field);
		}
		for (Map.Entry<String, ResultAggregator> v : varinationAggregatorsMap.entrySet()) {
			result.leftHeader.add(v.getKey());
			ArrayList<Double> row = new ArrayList<>();
			for (OperationOnField of : opFields) {
				ResultAggregator rg = v.getValue();
				Double value = 0.0;
				if (of.operation == AggregationOperation.SUM) {
					value = rg.sum(of.field);
				} else if (of.operation == AggregationOperation.AVG) {
					value = rg.average(of.field);
				}
				row.add(value);
			}
			result.content.add(row);
		}
		return result;
	}
}
