package edu.msu.cse.eventual.server;

import edu.msu.cse.dkvf.config.ConfigReader;

public class MainClass {
	public static void main(String args[]) {
		ConfigReader cnfReader = new ConfigReader(args[0]);
		EventualServer ss = new EventualServer(cnfReader);
		ss.runAll();
	}
}
