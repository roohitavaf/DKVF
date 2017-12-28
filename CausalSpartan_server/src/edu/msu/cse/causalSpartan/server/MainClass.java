package edu.msu.cse.causalSpartan.server;

import edu.msu.cse.dkvf.config.ConfigReader;

public class MainClass {
	public static void main(String args[]) {
		ConfigReader cnfReader = new ConfigReader(args[0]);
		CausalSpartanServer gServer = new CausalSpartanServer(cnfReader);
		gServer.runAll();
	}
}