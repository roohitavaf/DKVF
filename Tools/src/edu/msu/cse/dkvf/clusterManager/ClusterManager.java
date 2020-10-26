package edu.msu.cse.dkvf.clusterManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import java.net.Socket;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.ClusterReaderWriter;
import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.ClusterReaderWriter.ServerNode;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ExperimentReader;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ExperimentReader.YcsbClientNode;
import edu.msu.cse.gdkvp.controlMetadata.ControlMetadata.ControlMessage;
import edu.msu.cse.gdkvp.controlMetadata.ControlMetadata.ControlReply;
import edu.msu.cse.gdkvp.controlMetadata.ControlMetadata.StatusCheck;
import edu.msu.cse.gdkvp.controlMetadata.ControlMetadata.StatusCheckReply;
import edu.msu.cse.gdkvp.controlMetadata.ControlMetadata.Turnoff;
import edu.msu.cse.gdkvp.controlMetadata.ControlMetadata.TurnoffReply;

public class ClusterManager extends Manager {
	ClusterReaderWriter cr;
	ExperimentReader er;
	String errorMessage;
	PrintStream out;
	String serverConfigDir;
	String clientConfigDir;
	String experimentClientConfigDir;
	String experimentResultsDir;
	String expScriptDir;
	boolean strictHostKeyChecking = false; // debug change this later.

	class ServerStatus {
		String id;
		String ip;
		int rtt;
		int numOfServers;
		int numOfExpectedServers;
		int numOfClients;
	}

	final static String GENERATED_FILES = "generated_files";
	final static String SERVER_CONFIG_DIR = "server_configs";
	final static String CLIENT_CONFIG_DIR = "client_configs";
	final static String EXP_SCRIPT_DIR = "exp_scripts";
	final static String EXP_CLIENT_CONFIG_DIR = "exp_client_configs";
	final static String EXP_RESULTS_DIR = "exp_results";
	final static String EXP_LIBS_DIR = "libs";
	final static String CLIENT_CONFIG_NAME = "client_config";

	final static String OS_DEFAULT = "linux";
	final static String USERNAME_DEFAULT = "ubuntu";

	public ClusterManager(PrintStream out) {
		super(out);
		this.out = out;
	}

	/**
	 * 
	 * @param descriotorFile
	 * @return 0 success; 1 parsing error; 2 writing config files error
	 */
	public boolean loadCluster(String descriotorFile) {
		try {
			cr = new ClusterReaderWriter(descriotorFile);
			serverConfigDir = GENERATED_FILES + "/" + cr.getCluster().getName() + "/" + SERVER_CONFIG_DIR;
			clientConfigDir = GENERATED_FILES + "/" + cr.getCluster().getName() + "/" + CLIENT_CONFIG_DIR;
			return writeServerConfigFiles();
		} catch (Exception e) {
			setErrorMessage(e);
			return false;
		}
	}

	private boolean writeServerConfigFiles() {
		Utils.checkAndCreateDir(serverConfigDir);
		try {
			cr.writeServerConfigFiles(serverConfigDir);
			return true;
		} catch (JAXBException e) {
			printMessageln("Failed to create config files");
			setErrorMessage(e);
			return false;
		}
	}

	public boolean startNode(String id) {
		return startNode(cr.nodes.get(id));
	}

	public boolean uploadClusterToNode(String id, boolean uploadJar) {
		return uploadClusterToNode(cr.nodes.get(id), uploadJar);
	}

	public boolean uploadClusterToNode(ServerNode node, boolean uploadJar) {
		// connect to server
		try {
			node.connect(strictHostKeyChecking);
			//printMessageln(MessageFormat.format("Successfully connected to server {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to connect to server {0}:{1}", node.id, node.ip), e);
			return false;
		}

		// transfer files to server
		//we clean only when we want to send whole package including jar file
		if (uploadJar) {
			try {
				node.sshManager.cleanDir(node.workingDirectory);
				printMessageln(MessageFormat.format("Successfully created working directory at server {0}:{1}", node.id, node.ip));
			} catch (Exception e) {
				setErrorMessage(MessageFormat.format("Failed to create working directory at server {0}:{1}", node.id, node.ip), e);
				return false;
			}
		}
		String serverConfigFile = MessageFormat.format("{0}/server_{1}", serverConfigDir, node.id);
		if (uploadJar) {
			try {
				node.sshManager.uploadFile(node.jarFileAddress, node.workingDirectory + "/" + node.jarFileName);
				printMessageln(MessageFormat.format("Successfully uploaded server jar file to server {0}:{1}", node.id, node.ip));
			} catch (Exception e) {
				setErrorMessage(MessageFormat.format("Failed to upload server jar file to server {0}:{1}", node.id, node.ip), e);
				return false;
			}
		}
		try {
			node.sshManager.uploadFile(serverConfigFile, node.workingDirectory + "/server_" + node.id);
			printMessageln(MessageFormat.format("Successfully uploaded server config file to server {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to upload server config file to server {0}:{1}", node.id, node.ip), e);
			return false;
		}
		return true;
	}

	public boolean startNode(ServerNode node) {
		// connect to server
		try {
			node.connect(strictHostKeyChecking);
			//printMessageln(MessageFormat.format("Successfully connected to server {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to connect to server {0}:{1}", node.id, node.ip), e);
			return false;
		}

		// run the server
		String serverConfigFileName = MessageFormat.format("server_{0}", node.id);

		String command1 = MessageFormat.format("cd {0}", node.workingDirectory);
		String command2 = MessageFormat.format("java -jar {0} {1}", node.jarFileName, serverConfigFileName);

		String separator = ";";

		String command = MessageFormat.format("{0}{1}{2}", command1, separator, command2);

		try {
			node.sshManager.sendCommand(command);
			printMessageln(MessageFormat.format("Successfully requested server {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to run server {0}:{1}", node.id, node.ip), e);
			return false;
		}
		return true;
	}

	public boolean startCluster() {
		if (cr != null) {
			for (Map.Entry<String, ServerNode> nodeMap : cr.nodes.entrySet()) {
				ServerNode node = nodeMap.getValue();
				if (!startNode(node))
					return false;
			}
			return true;
		} else {
			setErrorMessage("First load a cluster using \"load_cluster\" command. Next, upload the cluster using \"upload_cluster\".");
			return false;
		}
	}

	public boolean uploadCluster(boolean uploadJar) {
		if (cr != null) {
			for (Map.Entry<String, ServerNode> nodeMap : cr.nodes.entrySet()) {
				ServerNode node = nodeMap.getValue();
				if (!uploadClusterToNode(node, uploadJar))
					return false;
			}
			return true;
		} else {
			setErrorMessage("First load a cluster using \"load_cluster\" command.");
			return false;
		}
	}

	public List<ServerStatus> clusterStatus() {
		if (cr.nodes != null) {
			List<ServerStatus> sss = new ArrayList<>();
			for (Map.Entry<String, ServerNode> nodeMap : cr.nodes.entrySet()) {
				ServerNode node = nodeMap.getValue();
				ServerStatus result = serverStatus(node);
				if (result != null)
					sss.add(result);
			}
			return sss;
		} else {
			setErrorMessage("No cluster is loaded. Please load a cluster first.");
			return null;
		}
	}

	public ServerStatus serverStatus(String id) {
		if (cr.nodes != null) {
			ServerNode node = cr.nodes.get(id);
			return serverStatus(node);
		} else {
			setErrorMessage("No cluster is loaded. Please load a cluster first.");
			return null;
		}
	}

	private ServerStatus serverStatus(ServerNode node) {
		if (node == null) {
			setErrorMessage("No such server exists.");
			return null;
		}
		Socket socket = null;
		try {
			socket = new Socket(node.ip, node.controlPort);
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			ControlMessage cm = ControlMessage.newBuilder().setStatusCheck(StatusCheck.newBuilder().build()).build();

			long startTime = System.currentTimeMillis();
			cm.writeDelimitedTo(out);
			ControlReply cr = ControlReply.parseDelimitedFrom(in);
			long endTime = System.currentTimeMillis();

			int rtt = (int) (endTime - startTime);

			if (cr.hasStatusCheckReply()) {
				StatusCheckReply scr = cr.getStatusCheckReply();

				ServerStatus ss = new ServerStatus();
				ss.id = node.id;
				ss.ip = node.ip;
				ss.rtt = rtt;
				ss.numOfClients = scr.getClients();
				ss.numOfServers = scr.getServers();
				ss.numOfExpectedServers = scr.getServersExpected();
				return ss;

			} else {
				setErrorMessage(MessageFormat.format("No staus check response for the server {0}:{1}", node.id, node.ip));
				return null;
			}

		} catch (Exception e) {
			setErrorMessage(e);
			return null;
		} finally {
			try {
				if (socket != null)
					socket.close();
			} catch (IOException e) {
				// don't care :/
			}
		}
	}

	public boolean stopCluster() {
		if (cr == null || cr.nodes == null)
			return true;
		for (Map.Entry<String, ServerNode> nodeMap : cr.nodes.entrySet()) {
			ServerNode node = nodeMap.getValue();
			stopServer(node);
		}
		printMessageln("Cluster sucessfully turned off");
		return true;
	}

	public boolean stopServer(String id) {
		if (cr.nodes != null) {
			ServerNode node = cr.nodes.get(id);
			return stopServer(node);
		} else {
			setErrorMessage("No cluster is loaded. Please load a cluster first.");
			return false;
		}
	}

	public boolean stopServer(ServerNode node) {
		if (node == null) {
			setErrorMessage("No such server exists.");
			return false;
		}
		Socket socket = null;
		try {
			socket = new Socket(node.ip, node.controlPort);
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();

			ControlMessage cm = ControlMessage.newBuilder().setTurnoff(Turnoff.newBuilder().build()).build();
			cm.writeDelimitedTo(out);
			ControlReply cr = ControlReply.parseDelimitedFrom(in);

			if (cr.hasTrunoffReply()) {
				TurnoffReply scr = cr.getTrunoffReply();
				return scr.getOk();

			} else {
				setErrorMessage(MessageFormat.format("No turnoff response for the server {0}:{1}", node.id, node.ip));
				return false;
			}

		} catch (Exception e) {
			setErrorMessage(e);
			return false;
		} finally {
			try {
				if (socket != null)
					socket.close();
			} catch (IOException e) {
				// don't care :/
			}
		}
	}

	public ClientManager connectToServer(String serverId, String stdLogLevel) {
		// first we create necessary config file for client
		try {
			ServerNode node = cr.nodes.get(serverId);
			if (node == null)
				return null;
			String configFile = cr.writeClientConfigFiles(serverId, stdLogLevel, clientConfigDir);
			return new ClientManager(node.clientClassName, configFile);
		} catch (Exception e) {
			setErrorMessage(e);
			return null;
		}
	}

	public ClientManager runClient(String configFile) {
		try {
			return new ClientManager(cr.getCluster().getDefaults().getClientClassName(), configFile);
		} catch (Exception e) {
			setErrorMessage(e);
			return null;
		}
	}

	public boolean killJava() {
		if (cr != null) {
			for (Map.Entry<String, ServerNode> nodeMap : cr.nodes.entrySet()) {
				ServerNode node = nodeMap.getValue();
				if (!killJavaNode(node))
					return false;
			}
			return true;
		} else {
			setErrorMessage("First load a cluster using \"load_cluster\" command.");
			return false;
		}
	}

	private boolean killJavaNode(ServerNode node) {
		// connect to server
		try {
			node.connect(strictHostKeyChecking);
			//printMessageln(MessageFormat.format("Successfully connected to server {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to connect to server {0}:{1}", node.id, node.ip), e);
			return false;
		}
		String command = "pkill -f 'java'";

		try {
			node.sshManager.sendCommand(command);
			printMessageln(MessageFormat.format("Successfully killed java at server {0}:{1}", node.id, node.ip));
		} catch (Exception e) {
			setErrorMessage(MessageFormat.format("Failed to kill java  at server {0}:{1}", node.id, node.ip), e);
			return false;
		}
		return true;
	}
}
