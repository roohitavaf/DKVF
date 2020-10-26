package edu.msu.cse.dkvf.clusterManager;

import java.util.ArrayList;

public class Table {
	ArrayList<String> topHeaders = new ArrayList<>();
	ArrayList<String> leftHeader = new ArrayList<>();
	ArrayList<ArrayList<Double>> content = new ArrayList<>();

	public String toCsv() {
		StringBuffer sb = new StringBuffer(",");
		for (String th : topHeaders) {
			sb.append(th).append(",");
		}
		sb.append("\n");
		for (int i = 0; i < content.size(); i++) {
			if (leftHeader.size() > i)
				sb.append(leftHeader.get(i)).append(",");
			for (int j = 0; j < topHeaders.size(); j++) {
				sb.append(content.get(i).get(j)).append(",");
			}
			sb.append("\n");
		}
		return sb.toString();
	}
}