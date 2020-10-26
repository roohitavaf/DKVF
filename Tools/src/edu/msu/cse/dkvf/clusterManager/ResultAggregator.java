package edu.msu.cse.dkvf.clusterManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ResultAggregator {

	public class Point {
		public double first;
		public double second;
	}

	Map<String, ResultsFileParser> fileNameToResults = new HashMap<>();

	public void readResults(ArrayList<File> fileList, ArrayList<String> measurementsToRead) throws FileNotFoundException {
		for (File file : fileList) {
			fileNameToResults.put(file.getName(), new ResultsFileParser(file.getPath(), measurementsToRead));
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (Map.Entry<String, ResultsFileParser> rfp : fileNameToResults.entrySet()) {
			sb.append(rfp.getKey()).append(":\n");
			sb.append(rfp.getValue().toString());
		}
		return sb.toString();
	}

	public Table readFields(ArrayList<String> fields) {
		Table result = new Table();
		for (String field : fields) {
			result.topHeaders.add(field);
		}
		for (Map.Entry<String, ResultsFileParser> rfp : fileNameToResults.entrySet()) {
			result.leftHeader.add(rfp.getKey());
			ArrayList<Double> row = new ArrayList<>();
			for (String field : fields){
				row.add(rfp.getValue().measurements.get(field.toLowerCase()));
			}
			result.content.add(row);
		}
		return result;
	}

	
	public Double sum (String field){
		if (fileNameToResults.size() < 1)
			return 0.0;
		Double sum  = 0.0;
		for (Map.Entry<String, ResultsFileParser> rfp : fileNameToResults.entrySet()) {
			if (rfp.getValue().measurements.containsKey(field.toLowerCase()))
				sum += rfp.getValue().measurements.get(field.toLowerCase());
			else return -1.0;
		}
		return sum;
	}
	
	public Double average (String field){
		if (fileNameToResults.size() < 1)
			return 0.0;
		Double sum = sum(field.toLowerCase());
		return sum/fileNameToResults.size();
	}
	
	
}
