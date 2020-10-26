package edu.msu.cse.dkvf.clusterManager;

import java.io.File;
import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Map;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import edu.msu.cse.dkvf.clusterManager.OperationOnField.AggregationOperation;
import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.ClusterReaderWriter.ServerNode;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ExperimentReader;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ExperimentWriter;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ExperimentReader.YcsbClientNode;

public class ExperimentManager extends Manager {
	PrintStream out;

	public ExperimentManager(PrintStream out) {
		super(out);
		this.out = out;
	}

	// Files and directory defaults
	final static String LOCAL_GENERATED_FILES = "generated_files";
	final static String LOCAL_EXP_SCRIPT_DIR = "exp_scripts";
	final static String LOCAL_EXP_CLIENT_CONFIG_DIR = "exp_client_configs";
	final static String LOCAL_EXP_RESULTS_DIR = "exp_results";
	final static String LOCAL_EXP_ERRORS_DIR = "exp_errors";
	final static String LOCAL_EXP_LIBS_DIR = "libs";

	public final static String REMOTE_EXP_LIBS_DIR = LOCAL_EXP_LIBS_DIR;
	public final static String REMOTE_EXP_RESULTS_DIR = LOCAL_EXP_RESULTS_DIR;
	public final static String REMOTE_EXP_ERRORS_DIR = LOCAL_EXP_ERRORS_DIR;
	public final static String REMOTE_CLIENT_CONFIG_NAME = "client_config";

	final static String LOAD = "load";
	final static String TRANSACTION = "t";

	public final static String ORIGINAL_VARIATION_NAME = "original";

	String clientConfigDir;
	String experimentClientConfigDir;
	String experimentResultsDir;
	String experimentErrorsDir;
	String expScriptDir;

	ExperimentReader er;
	ExperimentWriter ew;
	ResultsReader rr;

	boolean strictHostKeyChecking = false; // debug change this later.

	public boolean loadExperiment(String[] params) {
		String descriotorFile = params[1];
		boolean clean = false;
		if (params.length > 2 && params[2].equals("clean"))
			clean = true;

		try {
			er = new ExperimentReader(descriotorFile);
			ew = new ExperimentWriter(er.getExperiment(), er.nodes);

			experimentClientConfigDir = LOCAL_GENERATED_FILES + "/" + er.getExperiment().getName() + "/" + LOCAL_EXP_CLIENT_CONFIG_DIR;
			experimentResultsDir = LOCAL_GENERATED_FILES + "/" + er.getExperiment().getName() + "/" + LOCAL_EXP_RESULTS_DIR;
			experimentErrorsDir = LOCAL_GENERATED_FILES + "/" + er.getExperiment().getName() + "/" + LOCAL_EXP_ERRORS_DIR;
			expScriptDir = LOCAL_GENERATED_FILES + "/" + er.getExperiment().getName() + "/" + LOCAL_EXP_SCRIPT_DIR;

			// create necessary directories
			if (clean) {
				Utils.createOrClearDir(experimentClientConfigDir);
				Utils.createOrClearDir(experimentResultsDir);
				Utils.createOrClearDir(experimentErrorsDir);
				Utils.createOrClearDir(expScriptDir);
			} else {
				Utils.checkAndCreateDir(experimentClientConfigDir);
				Utils.checkAndCreateDir(experimentResultsDir);
				Utils.checkAndCreateDir(experimentErrorsDir);
				Utils.checkAndCreateDir(expScriptDir);
			}

			return writeExpClientConfigFiles() && writeExpScriptFiles();
		} catch (Exception e) {
			setErrorMessage(e);
			return false;
		}
	}

	private boolean writeExpScriptFiles() {
		try {
			ew.writeExpScriptFiles(expScriptDir);
			return true;
		} catch (Exception e) {
			setErrorMessage(e);
			return false;
		}
	}

	private boolean writeExpClientConfigFiles() {
		try {
			ew.writeExpClientConfigFiles(experimentClientConfigDir);
			return true;
		} catch (Exception e) {
			// debug
			e.printStackTrace();
			setErrorMessage(e);
			return false;
		}
	}

	public boolean uploadExpToNode(YcsbClientNode node, int uploadLevel) {

		// connect to server
		try {
			node.connect(strictHostKeyChecking);
			printMessageln(MessageFormat.format("Sucessfully connected to server {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to connect to ycsb client {0}:{1}", node.id, node.ip), e);
			return false;
		}

		// transfer files to server
		// creating working directory
		if (uploadLevel > 1) {
			try {
				node.sshManager.cleanDir(node.workingDirectory);
				printMessageln(MessageFormat.format("Sucessfully created working directory at client {0}:{1}", node.id, node.ip));
			} catch (Exception e) {
				setErrorMessage(MessageFormat.format("Failed to create working directory at ycsb client {0}:{1}", node.id, node.ip), e);
				return false;
			}
		}

		// creating results directory
		try {
			node.sshManager.cleanDir(node.workingDirectory + "/" + REMOTE_EXP_RESULTS_DIR);
			printMessageln(MessageFormat.format("Sucessfully created results directory at client {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to create working directory at ycsb client {0}:{1}", node.id, node.ip), e);
			return false;
		}

		// creating errors directory
		try {
			node.sshManager.cleanDir(node.workingDirectory + "/" + REMOTE_EXP_ERRORS_DIR);
			printMessageln(MessageFormat.format("Sucessfully created errors directory at client {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to create results directory at client {0}:{1}", node.id, node.ip), e);
			return false;
		}

		// uploading scripts
		try {
			File folder = new File(expScriptDir);
			File[] listOfFiles = folder.listFiles();
			if (listOfFiles != null) {
				for (int i = 0; i < listOfFiles.length; i++) {
					if (listOfFiles[i].isFile() && listOfFiles[i].getName().startsWith(MessageFormat.format("runClient_{0}", node.id))) {
						node.sshManager.uploadFile(listOfFiles[i].getPath(), node.workingDirectory + "/" + listOfFiles[i].getName());
					}
				}
			}
			//node.sshManager.uploadDirectoryContent(expScriptDir, node.workingDirectory);
			node.sshManager.changePermissionDirectoryContent(511, node.workingDirectory);
			printMessageln(MessageFormat.format("Sucessfully uploaded scripts to client {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to upload script files to client {0}:{1}", node.id, node.ip), e);
			return false;
		}

		// uploading config files
		String ycsbClientConfigFile = MessageFormat.format(ExperimentWriter.EXP_CLIENT_NAME_FORMAT, node.id);
		ycsbClientConfigFile = MessageFormat.format("{0}/{1}", experimentClientConfigDir, ycsbClientConfigFile);
		String destinationFile = MessageFormat.format("{0}/{1}", node.workingDirectory, REMOTE_CLIENT_CONFIG_NAME);
		try {
			node.sshManager.uploadFile(ycsbClientConfigFile, destinationFile);
			printMessageln(MessageFormat.format("Sucessfully uploaded ycsb client config file to client {0}:{1}", node.id, node.ip));
		} catch (JSchException | SftpException e) {
			setErrorMessage(new Exception(MessageFormat.format("Failed to upload ycsb client config file to client {0}:{1}. Source file: {2}, desination file: {3}", node.id, node.ip, ycsbClientConfigFile, destinationFile), e));
			return false;
		}

		try {
			node.sshManager.uploadFile(node.workloadAddress, node.workingDirectory + "/" + node.workloadName);
			printMessageln(MessageFormat.format("Sucessfully uploaded ycsb workload file to client {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to upload ycsb workload file to client {0}:{1}", node.id, node.ip));
			return false;
		}

		// uploading client jar file
		if (uploadLevel > 0) {
			try {
				node.sshManager.uploadFile(node.jarFileAddress, node.workingDirectory + "/" + node.jarFileName);
				printMessageln(MessageFormat.format("Sucessfully uploaded client jar file to client {0}:{1}", node.id, node.ip));
			} catch (Exception e) {
				setErrorMessage(new Exception(MessageFormat.format("Failed to upload client jar file to client {0}:{1}", node.id, node.ip), e));
				return false;
			}
		}

		// uploading libs directory
		if (uploadLevel > 1) {
			try {
				node.sshManager.uploadDirectory(LOCAL_EXP_LIBS_DIR, node.workingDirectory + "/" + LOCAL_EXP_LIBS_DIR);
				printMessageln(MessageFormat.format("Sucessfully uploaded libs directory to client{0}:{1}", node.id, node.ip));
			} catch (Exception e) {
				setErrorMessage(MessageFormat.format("Failed to upload libs directory to client {0}:{1}", node.id, node.ip));
				return false;
			}
		}
		return true;
	}

	public boolean uploadExperiment(String uploadLevelStr) {

		//Check level
		int uploadLevel = 0; // only uploads config filse + script files
		if (uploadLevelStr.equals("all"))
			uploadLevel = 2;
		else if (uploadLevelStr.equals("client"))
			uploadLevel = 1;
		else if (uploadLevelStr.equals("config"))
			uploadLevel = 0;
		else {
			setErrorMessage("Bad upload level. It must be in {all, client, config}");
			return false;
		}

		for (YcsbClientNode node : er.nodes.values()) {
			if (!uploadExpToNode(node, uploadLevel))
				return false;
		}
		return true;
	}

	public boolean downloadExpReults() {
		for (YcsbClientNode node : er.nodes.values()) {
			if (!downloadExpResultsFromNode(node))
				return false;
		}
		return true;
	}

	public boolean downloadExpResultsFromNode(YcsbClientNode node) {
		// connect to server
		try {
			node.connect(strictHostKeyChecking);
			//printMessageln(MessageFormat.format("Sucessfully connected to server {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to connect to ycsb client {0}:{1}", node.id, node.ip), e);
			return false;
		}

		// download the results
		try {
			node.sshManager.downloadDirectoryContent(node.workingDirectory + "/" + REMOTE_EXP_RESULTS_DIR, experimentResultsDir);
			return true;
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to download results from client {0}:{1}", node.id, node.ip), e);
			return false;
		}
	}

	public boolean downloadExpErrors() {
		for (YcsbClientNode node : er.nodes.values()) {
			if (!downloadExpErrorsFromNode(node))
				return false;
		}
		return true;
	}

	public boolean downloadExpErrorsFromNode(YcsbClientNode node) {
		// connect to server
		try {
			node.connect(strictHostKeyChecking);
			//printMessageln(MessageFormat.format("Sucessfully connected to server {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to connect to ycsb client {0}:{1}", node.id, node.ip), e);
			return false;
		}

		// download the results
		try {
			node.sshManager.downloadDirectoryContent(node.workingDirectory + "/" + REMOTE_EXP_ERRORS_DIR, experimentErrorsDir);
			return true;
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to download experiments from client {0}:{1}", node.id, node.ip), e);
			return false;
		}
	}

	public boolean startExperiment(int waitTime) {

		if (er == null) {
			printMessageln("First, load an experimetn using \"load_experiment\" command.");
			return false;
		}
		// Running original if there is not variation 
		if (er.variationNames.isEmpty()) {
			printMessageln(MessageFormat.format("Running experiments for variation={0}", ORIGINAL_VARIATION_NAME));
			if (startExperimentVariation(ORIGINAL_VARIATION_NAME))
				printMessageln(MessageFormat.format("Experiments for variation={0} is done", ORIGINAL_VARIATION_NAME));
			else {
				return false;
			}
		}
		// for each variation call startExperimentVariation
		for (String vName : er.variationNames) {
			printMessageln(MessageFormat.format("Running experiments for variation={0}", vName));
			
			if (startExperimentVariation(vName)){
				printMessageln(MessageFormat.format("Experiments for variation={0} is done", vName));
				try {
					Thread.sleep(waitTime);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else {
				return false;
			}
		}

		// download resutls
		if (!downloadExpReults())
			return false;
		else
			printMessageln("Results sucessfully downloaded");

		// download errors
		if (!downloadExpErrors())
			return false;
		else
			printMessageln("Error files sucessfully downloaded");
		return true;
	}

	boolean isValidVariationName(String variationName) {
		return er.variationNames.contains(variationName);
	}

	public boolean startExperimentVariation(String variationName) {
		// load phase
		printMessage("Load phase..................");
		if (startExperimentVariation(variationName, LOAD)) {
			printMessageln("done");
		} else
			return false;
		// transaction phase
		printMessage("Transaction phase...........");
		if (startExperimentVariation(variationName, TRANSACTION)) {
			printMessageln("done");
		} else
			return false;
		return true;
	}

	public boolean startExperimentVariation(String variationName, String phase) {
		try {
			ArrayList<CommandThread> cts = new ArrayList<>();
			for (YcsbClientNode node : er.nodes.values()) {
				//debug
				System.out.println("Running ycsb node");
				CommandThread ct = createThread(node, variationName, phase);
				if (ct == null)
					return false;
				ct.start();
				cts.add(ct);
			}
			for (CommandThread ct : cts) {
				ct.join();
				if (ct.getError() != null) {
					setErrorMessage(ct.getError());
					return false;
				}
			}
			return true;
		} catch (Exception e) {
			setErrorMessage(e);
			return false;
		}
	}

	public CommandThread createThread(YcsbClientNode node, String variationName, String phase) {
		if (variationName == null)
			variationName = ORIGINAL_VARIATION_NAME;
		if (!isValidVariationName(variationName) && !variationName.equals(ORIGINAL_VARIATION_NAME)) {
			setErrorMessage(MessageFormat.format("No such variation: {0}", variationName));
			return null;
		}
		if (!(phase.equals(LOAD) || phase.equals(TRANSACTION))) {
			setErrorMessage("No such phase. Phase can be \"t\" or \"load\".");
			return null;
		}
		String cdCommand = MessageFormat.format("cd {0}", node.workingDirectory);
		String scriptCommand;
		String separator = ";";
		if (node.os.equals("windows")) {
			if (phase.equals(TRANSACTION)) {
				scriptCommand = MessageFormat.format(ExperimentWriter.BAT_NAME_FORMAT_T, node.id, variationName);
			} else {
				scriptCommand = MessageFormat.format(ExperimentWriter.BAT_NAME_FORMAT_LOAD, node.id, variationName);
			}
		} else {
			if (phase.equals(TRANSACTION)) {
				scriptCommand = MessageFormat.format(ExperimentWriter.SH_NAME_FORMAT_T, node.id, variationName);
			} else {
				scriptCommand = MessageFormat.format(ExperimentWriter.SH_NAME_FORMAT_LOAD, node.id, variationName);
			}
		}
		String command = MessageFormat.format("{0} {1} {2}", cdCommand, separator, node.workingDirectory + "/" + scriptCommand);
		CommandThread ct = new CommandThread();
		ct.init(command, node, strictHostKeyChecking, out);
		return ct;
	}

	// Results section
	public boolean parseResults(ArrayList<String> measurementsList) {
		rr = new ResultsReader();
		if (measurementsList == null)
			measurementsList = new ArrayList<String>();
		for (int i = 0; i < measurementsList.size(); i++) {
			measurementsList.set(i, measurementsList.get(i).toLowerCase());
		}
		// adding default measurements:
		if (!measurementsList.contains("throughput"))
			measurementsList.add("throughput");
		if (!measurementsList.contains("insert"))
			measurementsList.add("insert");
		if (!measurementsList.contains("read"))
			measurementsList.add("read");
		try {
			rr.readDir(experimentResultsDir, measurementsList, "_t.result");
			return true;
		} catch (Exception e) {
			setErrorMessage(e);
			return false;
		}
	}

	public boolean readVarResults(ArrayList<String> nameAndFields) {
		try {
			String variationName = ORIGINAL_VARIATION_NAME;
			if (!nameAndFields.isEmpty()) {
				variationName = nameAndFields.get(0);
			}
			if (!nameAndFields.isEmpty())
				nameAndFields.remove(0);
			// adding default files if the list is empty:
			if (nameAndFields.isEmpty()) {
				nameAndFields.add("throughput");
				nameAndFields.add("insert.averagelatency");
				nameAndFields.add("read.averagelatency");
			}
			Table result = rr.readVarReuslts(variationName, nameAndFields);
			StringBuffer sb = new StringBuffer(variationName).append(".");
			for (String s : nameAndFields) {
				sb.append(s).append(".");
			}
			sb.append("csv");
			String csvName = sb.toString();
			String csvAddress = MessageFormat.format("{0}/{1}", experimentResultsDir, csvName);
			// saving csv
			String csvContent = result.toCsv();
			Utils.createTextFile(csvContent, csvAddress);
			printMessageln(MessageFormat.format("Results save in {0}", csvAddress));
			return true;
		} catch (Exception e) {
			setErrorMessage(e);
			return false;
		}
	}

	public boolean readAggResults(ArrayList<String> params) {
		try {
			if (params.isEmpty()) {
				OperationOnField sumThroughput = new OperationOnField(AggregationOperation.SUM, "throughput");
				OperationOnField avgInsertLatency = new OperationOnField(AggregationOperation.AVG, "insert.averagelatency");
				OperationOnField avgReadLatency = new OperationOnField(AggregationOperation.AVG, "read.averagelatency");
				Table aggResults = rr.readAggResults(new OperationOnField[] { sumThroughput, avgInsertLatency, avgReadLatency });
				String csvName =  er.getExperiment().getName() + "_agg_results.sumThroughput.avgInsertLatency.avgReadLatency.csv";
				String csvContent = aggResults.toCsv();
				// debug
				System.out.println(csvContent);
				String csvAddress = MessageFormat.format("{0}/{1}", experimentResultsDir, csvName);
				Utils.createTextFile(csvContent, csvAddress);
				printMessageln(MessageFormat.format("Results saved in {0}", csvAddress));
				return true;
			} else {
				if (params.size() % 2 == 1) {
					setErrorMessage(new IllegalArgumentException());
					return false;
				}
				OperationOnField[] ofs = new OperationOnField[params.size() / 2];
				for (int i = 0; i < params.size(); i += 2) {
					AggregationOperation ao;
					if (params.get(i).toLowerCase().equals("sum")) {
						ao = AggregationOperation.SUM;
					} else if (params.get(i).toLowerCase().equals("avg")) {
						ao = AggregationOperation.AVG;
					} else {
						setErrorMessage(new IllegalArgumentException());
						return false;
					}
					OperationOnField of = new OperationOnField(ao, params.get(i + 1).toLowerCase());
					ofs[i / 2] = of;
				}
				Table aggResults = rr.readAggResults(ofs);
				StringBuffer sb = new StringBuffer("agg_results.");
				for (OperationOnField of : ofs) {
					sb.append(of.field).append(".");
				}
				sb.append("csv");

				String csvName = sb.toString();
				String csvContent = aggResults.toCsv();
				// debug
				System.out.println(csvContent);
				String csvAddress = MessageFormat.format("{0}/{1}", experimentResultsDir, csvName);
				Utils.createTextFile(csvContent, csvAddress);
				printMessageln(MessageFormat.format("Results saved in {0}", csvAddress));
				return true;
			}
		} catch (Exception e) {
			setErrorMessage(e);
			return false;
		}

	}

	public boolean parseResultsDir(ArrayList<String> measurementsList) {
		rr = new ResultsReader();
		String dir = measurementsList.get(0);
		measurementsList.remove(0);
		if (measurementsList.isEmpty())
			measurementsList = new ArrayList<String>();
		for (int i = 0; i < measurementsList.size(); i++) {
			measurementsList.set(i, measurementsList.get(i).toLowerCase());
		}
		// adding default measurements:
		if (!measurementsList.contains("throughput"))
			measurementsList.add("throughput");
		if (!measurementsList.contains("insert"))
			measurementsList.add("insert");
		if (!measurementsList.contains("read"))
			measurementsList.add("read");
		try {
			rr.readDir(dir, measurementsList, "_t.result");
			experimentResultsDir = dir;
			return true;
		} catch (Exception e) {
			setErrorMessage(e);
			return false;
		}
	}

	public boolean killJava() {
		if (er != null) {
			for (Map.Entry<String, YcsbClientNode> nodeMap : er.nodes.entrySet()) {
				YcsbClientNode node = nodeMap.getValue();
				if (!killJavaNode(node))
					return false;
			}
			return true;
		} else {
			setErrorMessage("First load an experiment using \"load_experiment\" command.");
			return false;
		}
	}

	private boolean killJavaNode(YcsbClientNode node) {
		// connect to server
		try {
			node.connect(strictHostKeyChecking);
			//printMessageln(MessageFormat.format("Successfully connected to server {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to connect to client {0}:{1}", node.id, node.ip), e);
			return false;
		}
		String command = "pkill -f 'java'";

		try {
			node.sshManager.sendCommand(command);
			printMessageln(MessageFormat.format("Successfully killed java at client {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to kill java  at client {0}:{1}", node.id, node.ip), e);
			return false;
		}
		return true;
	}
}
