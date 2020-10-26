package edu.msu.cse.dkvf.clusterManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import edu.msu.cse.dkvf.clusterManager.ClusterManager.ServerStatus;

public class UI {
	static ClusterManager clusterManager = new ClusterManager(System.out);
	static ExperimentManager experimentManager = new ExperimentManager(System.out);
	static Scanner scanner;

	public static void main(String args[]) {
		try {
			byte[] encoded = Files.readAllBytes(Paths.get("banner.txt"));
			String banner = new String(encoded);

			System.out.println(banner);

		} catch (IOException e) {
			System.out.println("-------------------------------------");
			System.out.println("-----------DKVF ClusterManager-------");
			System.out.println("-------------------------------------");
		}
		System.out.println("");

		System.out.println("Welcome to DKVF ClusterManager! Type \"help\" for help.");

		String command = "";
		scanner = new Scanner(System.in);
		while (!command.equals("exit")) {
			System.out.print("> ");
			command = scanner.nextLine().trim();

			// System.out.println("Reading from consol");
			// command = System.console().readLine();
			String[] commandParts = command.split("\\s+");
			switch (commandParts[0]) {
			case "kill_java_cluster": 
				killJavaCluster(commandParts);
				break;
			case "kill_java_experiment": 
				killJavaExperiment(commandParts);
				break;	
			case "parse_results": 
				parseResults(commandParts);
				break;
			case "parse_results_dir": 
				parseResultsDir(commandParts);
				break;
			case "read_agg_results":
				readAggResults (commandParts);
				break;
			case "read_var_results":
				readVarResults(commandParts);
				break;
			case "upload_experiment":
				uploadExperiment(commandParts);
				break;
			case "start_experiment":
				startExperiment(commandParts);
				break;
			case "load_experiment":
				loadExperiment(commandParts);
				break;
			case "load_cluster":
				loadCluster(commandParts);
				break;
			case "start_cluster":
				startCluster(commandParts);
				break;
			case "upload_cluster":
				uploadCluster(commandParts);
				break;
			case "stop_server":
				stopServer(commandParts);
				break;
			case "stop_cluster":
				stopCluster(commandParts);
				break;
			case "cluster_status":
				clusterStatus(commandParts);
				break;
			case "server_status":
				serverStatus(commandParts);
				break;
			case "connect_server":
				connectServer(commandParts);
				break;
			case "run_client":
				runClient(commandParts);
				break;
			case "read_cluster_error":
				readClusterError();
				break;
			case "read_experiment_error":
				readExperimentError();
				break;
			case "less":
				clusterManager.out = null;
				break;
			case "more":
				clusterManager.out = System.out;
				break;
			case "exit":
				exit();
				break;
			case "":
				break;
			default:
				System.out.println("Not recognized " + commandParts[0] + ". Type help for help.");
			}
		}
		scanner.close();
	}

	private static void killJavaExperiment(String[] commandParts) {
		if (experimentManager.killJava())
			System.out.println("Kill java ran sucessfully on cluster nodes.");
		else
			System.out.println("Kill java failed. Type \"read_experiment_error\" to see the error.");
		
		
	}

	private static void killJavaCluster(String[] commandParts) {
		if (clusterManager.killJava())
			System.out.println("Kill java ran sucessfully on experiment nodes.");
		else
			System.out.println("Starting the cluster failed. Type \"read_cluster_error\" to see the error.");
		
	}

	private static void uploadCluster(String[] commandParts) {
		boolean uploadJar = true;
		if (commandParts.length > 1) {
			System.out.println("Uploading without jar file.");
			uploadJar = false;
		}
		if (clusterManager.uploadCluster(uploadJar))
			System.out.println("Cluster files uploaded sucessfully. Use \"start_cluster\" to run the cluster.");
		else
			System.out.println("Starting the cluster failed. Type \"read_cluster_error\" to see the error.");
	}

	private static void parseResultsDir(String[] commandParts) {
		if (commandParts.length < 2) {
			System.out.println("Please specify results dir to parse.");
			return;
		}
		ArrayList<String> measurementsList = new ArrayList<String>(Arrays.asList(commandParts));
		measurementsList.remove(0);
		if (experimentManager.parseResultsDir(measurementsList))
			System.out.println("Sucessfully parsed result files. Use \"read_agg_results\" or \"read_var_results\" to read the results.");
		else
			System.out.println("Failed to parse result files. Type \"read_experiment_error\" to see the error.");
		
		
	}

	private static void readVarResults(String[] commandParts) {
		ArrayList<String> nameAndFields = new ArrayList<String>(Arrays.asList(commandParts));
		nameAndFields.remove(0);
		if (!experimentManager.readVarResults(nameAndFields))
			System.out.println("Reading variation results failed. Type \"read_experiment_error\" to see the error.");
	}

	private static void parseResults(String[] commandParts) {
		ArrayList<String> measurementsList = new ArrayList<String>(Arrays.asList(commandParts));
		measurementsList.remove(0);
		if (experimentManager.parseResults(measurementsList))
			System.out.println("Sucessfully parsed result files. Use \"read_agg_results\" or \"read_var_results\" to read the results.");
		else
			System.out.println("Failed to parse result files. Type \"read_experiment_error\" to see the error.");
	}

	private static void readAggResults(String[] commandParts) {
		ArrayList<String> params = new ArrayList<String>(Arrays.asList(commandParts));
		params.remove(0);
		if (!experimentManager.readAggResults(params))
			System.out.println("Failed to aggregate results. Type \"read_experiment_error\" to see the error.");
	}

	private static void uploadExperiment (String[] commandParts){
		if (commandParts.length < 2) {
			System.out.println("Please specify upload level (config, client, or all).");
			return;
		}
		
		if (experimentManager.uploadExperiment(commandParts[1]))
			System.out.println("Sucessfully uploaded experiment files. Use \"start_experiment\" to run the experiment.");
		else
			System.out.println("Failed to upload experiment files. Type \"read_experiment_error\" to see the error.");
	}
	private static void startExperiment(String[] commandParts) {
		int waitBetweenExperiments = 0;
		try {
			 waitBetweenExperiments = new Integer (commandParts[1]);
		}catch (Exception e){
			System.out.println("Give the wait time between experiments in milliseconds");
			return;
		}
		if (experimentManager.startExperiment(waitBetweenExperiments))
			System.out.println("Experiment finished sucessfully. Use \"parse_results\" command to parse the results.");
		else
			System.out.println("Starting the experiment failed. Type \"read_experiment_error\" to see the error.");
	}

	private static void loadExperiment(String[] commandParts) {
		if (commandParts.length < 2) {
			System.out.println("Please specify the experiment file.");
			return;
		}
		if (experimentManager.loadExperiment(commandParts))
			System.out.println("Experiment loaded sucessfully. \nUse \"upload_experiment\" to upload necessary files to clients, \nor use \"start_experiment\" command to run the experiment.");
		else
			System.out.println("Loading experiment failed. Type \"read_experiment_error\" to see the error.");
	}

	private static void stopServer(String[] commandParts) {
		if (commandParts.length < 2) {
			System.out.println("Please specify the server id.");
			return;
		}
		boolean result = clusterManager.stopServer(commandParts[1]);
		if (!result) {
			System.out.println("Problem stopping server. Type \"read_cluster_error\" to see the error.");
			return;
		}
		System.out.println("Server sucessfully stopped.");
	}

	private static void exit() {
		if (clusterManager != null && clusterManager.stopCluster()) {
			System.exit(0);
		} else {
			System.out.println("Problem in exit. Type \"read_cluster_error\" to see the error.");
		}

	}

	private static void serverStatus(String[] commandParts) {
		if (commandParts.length < 2) {
			System.out.println("Please specify the server id.");
			return;
		}
		ServerStatus ss = clusterManager.serverStatus(commandParts[1]);
		if (ss == null) {
			System.out.println("Problem reading server status. Type \"read_cluster_error\" to see the error.");
			return;
		}
		printServerStatusWithHeader(ss);
	}

	private static void clusterStatus(String[] commandParts) {
		List<ServerStatus> clusterStatus = clusterManager.clusterStatus();

		if (clusterStatus != null)
			printClusterStatus(clusterStatus);
		else
			System.out.println("Reading cluster status failed. Type \"read_cluster_error\" to see the error.");
	}

	private static void printClusterStatus(List<ServerStatus> sss) {
		printStatusHeader();
		for (ServerStatus ss : sss)
			printServerStatus(ss);
	}

	private static void printStatusHeader() {
		System.out.println(String.format("%4s%1s%15s%1s%15s%1s%15s%1s%15s", "ID", "|", "IP", "|", "RTT (ms)", "|", "#Servers", "|", "#Clients"));
		System.out.println(String.format("%s", "---------------------------------------------------------------------"));
	}

	private static void printServerStatusWithHeader(ServerStatus ss) {
		printStatusHeader();
		printServerStatus(ss);
	}

	private static void printServerStatus(ServerStatus ss) {
		System.out.println(String.format("%4s%1s%15s%1s%15s%1s%15s%1s%15s", ss.id, "|", ss.ip, "|", ss.rtt, "|", ss.numOfServers + "/" + ss.numOfExpectedServers, "|", ss.numOfClients));
	}

	private static void stopCluster(String[] commandParts) {
		if (clusterManager.stopCluster()) {
			System.out.println("Sucessfully turned off the cluster");
		} else {
			System.out.println("Problem in stopping cluster. Type \"read_cluster_error\" to see the error.");
		}

	}

	private static void readClusterError() {
		if (clusterManager.getErrorMessage() != null)
			System.out.println(clusterManager.getErrorMessage());
		else
			System.out.println("No error message exists to show.");

	}
	
	private static void readExperimentError() {
		if (experimentManager.getErrorMessage() != null)
			System.out.println(experimentManager.getErrorMessage());
		else
			System.out.println("No error message exists to show.");

	}

	private static void connectServer(String[] commandParts) {
		if (commandParts.length < 3) {
			System.out.println("Please the server id and log level.");
			return;
		}

		ClientManager cm = clusterManager.connectToServer(commandParts[1], commandParts[2]);

		if (cm != null) {
			UIClient uic = new UIClient(cm, commandParts[1]);
			uic.runUI(scanner);
			cm.close();
		} else {
			System.out.println("Problem in creating client and connect to server. Type \"read_cluster_error\" to see the error.");
		}
	}
	
	private static void runClient(String[] commandParts) {
		if (commandParts.length < 2) {
			System.out.println("Please provide the the client config file.");
			return;
		}
		ClientManager cm = clusterManager.runClient(commandParts[1]);

		if (cm != null) {
			UIClient uic = new UIClient(cm, commandParts[1]);
			uic.runUI(scanner);
			cm.close();
		} else {
			System.out.println("Problem in running the client. Type \"read_cluster_error\" to see the error.");
		}
	}

	private static void loadCluster(String[] commandParts) {
		if (commandParts.length < 2) {
			System.out.println("Please specify the cluster discriptor file.");
			return;
		}
		if (clusterManager.loadCluster(commandParts[1]))
			System.out.println("Cluster loaded sucessfully. Use \"upload_cluster\" command to upload cluster files.");
		else
			System.out.println("Loading cluster failed. Type \"read_cluster_error\" to see the error.");
	}

	private static void startCluster(String[] commandParts) {
		// we first stop and then start to make sure previous cluster is truned
		// off
		stopCluster(commandParts);
		if (clusterManager.startCluster())
			System.out.println("Cluster requested sucessfully. Use \"cluster_status\" command to check the status of the cluter.");
		else
			System.out.println("Starting the cluster failed. Type \"read_cluster_error\" to see the error.");
	}



}
