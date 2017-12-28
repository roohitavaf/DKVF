package edu.msu.cse.cops.server;

import edu.msu.cse.dkvf.config.ConfigReader;

public class MainClass {
	public static void main(String args[]) {
		ConfigReader cnfReader = new ConfigReader(args[0]);
		COPSServer gServer = new COPSServer(cnfReader);
		gServer.runAll();
	}
}