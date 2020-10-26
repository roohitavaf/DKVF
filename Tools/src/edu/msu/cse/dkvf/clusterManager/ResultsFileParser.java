package edu.msu.cse.dkvf.clusterManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ResultsFileParser {
	Map<String, Double> measurements = new HashMap<>();

	public ResultsFileParser(String resultFileAddress, ArrayList<String> measurementsToRead) throws FileNotFoundException {
		Scanner scan = new Scanner(new File(resultFileAddress));
		while (scan.hasNext()) {
			String line = scan.nextLine().toLowerCase().toString();
			for (String m : measurementsToRead){
				String lM = m.toLowerCase();
				if (line.contains(lM)){
					String[] lineParts = line.split(",");
					if (lM.equals("throughput")){
						String key = trimMeasurementName(lineParts[1]);
						measurements.put(key, new Double(lineParts[2].trim()));
					}else {
						if (lineParts.length > 2 && !Utils.isNumeric(trimMeasurementName(lineParts[1]))){
							String key = String.format("%s.%s", m, lineParts[1].trim());
							key = trimMeasurementName(key);
							measurements.put(key, new Double(lineParts[2].trim()));
						}
					}			
				}
			}
		}
		scan.close();
	}
	
	private String trimMeasurementName (String name){
		name = name.trim();
		if (name.contains("("))
			name = name.substring(0, name.indexOf("("));
		return name;
	}
	
	public String toString (){
		StringBuffer sb = new StringBuffer();
		for (Map.Entry<String, Double> entry : measurements.entrySet()){
			sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
		}
		return sb.toString();
	}
	
	
	
}
