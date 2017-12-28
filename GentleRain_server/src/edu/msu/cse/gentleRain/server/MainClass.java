package edu.msu.cse.gentleRain.server;

import edu.msu.cse.dkvf.config.ConfigReader;

public class MainClass {
	public static void main(String args[]) {
		ConfigReader cnfReader = new ConfigReader(args[0]);
		GentleRainServer gServer = new GentleRainServer(cnfReader);
		gServer.runAll();
	}
}
