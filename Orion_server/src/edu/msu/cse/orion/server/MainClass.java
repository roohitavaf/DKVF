package edu.msu.cse.orion.server;

import edu.msu.cse.dkvf.config.ConfigReader;

public class MainClass {
    public static void main(String args[]) {
        ConfigReader cnfReader = new ConfigReader(args[0]);
        OrionServer gServer = new OrionServer(cnfReader);
        gServer.runAll();
    }
}