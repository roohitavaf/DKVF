package edu.msu.cse.accf.server;

import edu.msu.cse.dkvf.config.ConfigReader;

public class MainClass {
	public static void main(String args[]) {
		ConfigReader cnfReader = new ConfigReader(args[0]);
		ACCFServer gServer = new ACCFServer(cnfReader);
		gServer.runAll();
	}
}