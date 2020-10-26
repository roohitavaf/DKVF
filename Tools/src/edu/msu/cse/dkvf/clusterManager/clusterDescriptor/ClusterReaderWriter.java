package edu.msu.cse.dkvf.clusterManager.clusterDescriptor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import com.jcraft.jsch.JSchException;

import edu.msu.cse.dkvf.clusterManager.SSHManager;
import edu.msu.cse.dkvf.clusterManager.Utils;
import edu.msu.cse.dkvf.clusterManager.config.ConfigWriter;
import edu.msu.cse.dkvf.clusterManager.config.ConnectTo;
import edu.msu.cse.dkvf.clusterManager.config.Config;
import edu.msu.cse.dkvf.clusterManager.config.Config.ProtocolProperties;
import edu.msu.cse.dkvf.clusterManager.config.ServerInfo;
import edu.msu.cse.dkvf.clusterManager.config.Storage;

public class ClusterReaderWriter {

	public class ServerNode {
		public String id;
		public String ip;
		public String serverPort;
		public int controlPort;
		public int clientPort;
		public String userName;
		public String password;
		public String key;
		public String jarFileAddress;
		public String jarFileName;
		public String clientClassName;
		public String workingDirectory;
		public SSHManager sshManager;
		public String connectorSleepTime;
		public String channelCapacity;
		public String storageClassName;
		public List<edu.msu.cse.dkvf.clusterManager.config.Property> storageProperties;
		public List<edu.msu.cse.dkvf.clusterManager.config.Property> protocolProperties;

		public String clientStorageClassName;
		public List<edu.msu.cse.dkvf.clusterManager.config.Property> clientStorageProperties;
		public List<edu.msu.cse.dkvf.clusterManager.config.Property> clientProtocolProperties;

		//log properties:
		public String protocolLogFile;
		public String protocolLogLevel;
		public String protocolLogType;
		public String protocolStdLogLevel;
		public String frameworkLogFile;
		public String frameworkLogLevel;
		public String frameworkLogType;
		public String frameworkStdLogLevel;

		public void connect(boolean strictHostKeyChecking) throws JSchException {
			if (sshManager == null || !sshManager.isConnected()) {
				SSHManager instance = null;
				instance = new SSHManager(userName, password, ip, "");
				if (key != null)
					instance.setPrivateKey(key);
				instance.connect(strictHostKeyChecking);
				sshManager = instance;
			}
		}
	}

	//private HashMap<String, ServerInfo> serverInfos;
	private Cluster cluster;
	public HashMap<String, ServerNode> nodes;
	private HashMap<String, HashMap<String, ServerNode>> neighborsMap;

	final static String CLUSTER_XSD_FILE = "XSDs/ClusterDescriptor.xsd";

	final static int CONTROL_PORT_DEFAULT = 2001;
	final static int CLIENT_PORT_DEFAULT = 2002;

	public ClusterReaderWriter(String fileAddress) throws JAXBException, SAXException {
		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

		InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(CLUSTER_XSD_FILE);
		Schema schema = schemaFactory.newSchema(new StreamSource(in));

		File file = new File(fileAddress);
		JAXBContext jaxbContext = JAXBContext.newInstance(Cluster.class);

		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		jaxbUnmarshaller.setSchema(schema);

		setCluster((Cluster) jaxbUnmarshaller.unmarshal(file));
	}

	private void readNodes() {
		nodes = new HashMap<>();
		for (Server server : cluster.getServers().getServer()) {
			ServerNode node = new ServerNode();

			node.id = server.getId();
			node.ip = server.getIp();

			node.jarFileAddress = setProperty(cluster.getDefaults().getServerJarFile(), server.getConfig().getServerJarFile());
			if (node.jarFileAddress == null)
				throw new RuntimeException("You have to provide server_jar_file!");
			node.jarFileName = node.jarFileAddress.substring(node.jarFileAddress.lastIndexOf("/") + 1);
			node.clientClassName = setProperty(cluster.getDefaults().getClientClassName(), server.getConfig().getClientClassName());
			node.workingDirectory = setProperty(cluster.getDefaults().getWorkingDirectory(), server.getConfig().getWorkingDirectory()).trim();

			node.userName = setProperty(cluster.getDefaults().getUsername(), server.getConfig().getUsername());
			node.password = setProperty(cluster.getDefaults().getPassword(), server.getConfig().getPassword());
			node.key = setProperty(cluster.getDefaults().getKey(), server.getConfig().getKey());

			node.serverPort = setProperty(cluster.getDefaults().getServerPort(), server.getConfig().getServerPort());

			String controlPortStr = setProperty(cluster.getDefaults().getControlPort(), server.getConfig().getControlPort());
			node.controlPort = CONTROL_PORT_DEFAULT;
			if (controlPortStr != null)
				node.controlPort = new Integer(controlPortStr.trim());

			String clientPortStr = setProperty(cluster.getDefaults().getClientPort(), server.getConfig().getClientPort());
			node.clientPort = CLIENT_PORT_DEFAULT;
			if (clientPortStr != null)
				node.clientPort = new Integer(clientPortStr.trim());

			String clusterStorageClassName = null;
			String serverStorageClassName = null;
			if (cluster.getDefaults().getStorage() != null)
				clusterStorageClassName = cluster.getDefaults().getStorage().getClassName();
			if (server.getConfig().getStorage() != null)
				serverStorageClassName = server.getConfig().getStorage().getClassName();
			node.storageClassName = setProperty(clusterStorageClassName, serverStorageClassName);

			List<Property> clusterStorageProperties = null;
			List<Property> serverStorageProperties = null;
			if (cluster.getDefaults().getStorage() != null)
				clusterStorageProperties = cluster.getDefaults().getStorage().getProperty();
			if (server.getConfig().getStorage() != null)
				serverStorageProperties = server.getConfig().getStorage().getProperty();
			node.storageProperties = setProperties(clusterStorageProperties, serverStorageProperties);
			//--
			String clientClusterStorageClassName = null;
			String clientServerStorageClassName = null;
			if (cluster.getDefaults().getClientStorage() != null)
				clientClusterStorageClassName = cluster.getDefaults().getClientStorage().getClassName();
			if (server.getConfig().getClientStorage() != null)
				clientServerStorageClassName = server.getConfig().getClientStorage().getClassName();
			node.clientStorageClassName = setProperty(clientClusterStorageClassName, clientServerStorageClassName);

			List<Property> clientClusterStorageProperties = null;
			List<Property> clientServerStorageProperties = null;
			if (cluster.getDefaults().getClientStorage() != null)
				clientClusterStorageProperties = cluster.getDefaults().getClientStorage().getProperty();
			if (server.getConfig().getClientStorage() != null)
				clientServerStorageProperties = server.getConfig().getClientStorage().getProperty();
			node.clientStorageProperties = setProperties(clientClusterStorageProperties, clientServerStorageProperties);

			//---
			node.connectorSleepTime = setProperty(cluster.getDefaults().getConnectorSleepTime(), server.getConfig().getConnectorSleepTime());
			node.channelCapacity = setProperty(cluster.getDefaults().getChannelCapacity(), server.getConfig().getChannelCapacity());

			node.protocolLogFile = setProperty(cluster.getDefaults().getProtocolLogFile(), server.getConfig().getProtocolLogFile());
			node.protocolLogLevel = setProperty(cluster.getDefaults().getProtocolLogLevel(), server.getConfig().getProtocolLogLevel());
			node.protocolLogType = setProperty(cluster.getDefaults().getProtocolLogType(), server.getConfig().getProtocolLogType());
			node.protocolStdLogLevel = setProperty(cluster.getDefaults().getProtocolStdLogLevel(), server.getConfig().getProtocolStdLogLevel());

			node.frameworkLogFile = setProperty(cluster.getDefaults().getFrameworkLogFile(), server.getConfig().getFrameworkLogFile());
			node.frameworkLogLevel = setProperty(cluster.getDefaults().getFrameworkLogLevel(), server.getConfig().getFrameworkLogLevel());
			node.frameworkLogType = setProperty(cluster.getDefaults().getFrameworkLogType(), server.getConfig().getFrameworkLogType());
			node.frameworkStdLogLevel = setProperty(cluster.getDefaults().getFrameworkStdLogLevel(), server.getConfig().getFrameworkStdLogLevel());

			//protocol_properties
			List<Property> clusterProtocolProperties = null;
			List<Property> serverProtocolProperties = null;
			if (cluster.getDefaults().getProtocolProperties() != null)
				clusterProtocolProperties = cluster.getDefaults().getProtocolProperties().getProperty();
			if (server.getConfig().getProtocolProperties() != null)
				serverProtocolProperties = server.getConfig().getProtocolProperties().getProperty();
			node.protocolProperties = setProperties(clusterProtocolProperties, serverProtocolProperties);

			//client_protocol_properties
			List<Property> clusterClientProtocolProperties = null;
			List<Property> serverClientProtocolProperties = null;
			if (cluster.getDefaults().getClientProtocolProperties() != null)
				clusterClientProtocolProperties = cluster.getDefaults().getClientProtocolProperties().getProperty();
			if (server.getConfig().getClientProtocolProperties() != null)
				serverClientProtocolProperties = server.getConfig().getClientProtocolProperties().getProperty();
			node.clientProtocolProperties = setProperties(clusterClientProtocolProperties, serverClientProtocolProperties);

			nodes.put(node.id, node);
		}
	}

	public Cluster getCluster() {
		return cluster;

	}

	/*
	public HashMap<String, ServerInfo> getServerInfos() {
		return serverInfos;
	}*/

	public HashMap<String, HashMap<String, ServerNode>> getNeighborsMap() {
		return neighborsMap;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
		if (cluster.getDefaults() == null)
			cluster.setDefaults(new Parameters());
		//readServers();
		readNodes();
		readTopology();

	}

	/*
	private void readServers() {
		serverInfos = new HashMap<>();
		for (Server s : cluster.getServers().getServer()) {
			if (s.getConfig() == null)
				s.setConfig(new Parameters());
			if (serverInfos.containsKey(s.getId()))
				continue;
			ServerInfo si = new ServerInfo();
			si.setId(s.getId());
			si.setIp(s.getIp());
			String serverPort = setProperty(cluster.getDefaults().getServerPort(), s.getConfig().getServerPort());
			si.setPort(serverPort);
			if (s.getConfig() != null && s.getConfig().getServerPort() != cluster.getDefaults().getServerPort()) {
				si.setPort(s.getConfig().getServerPort());
			}
			serverInfos.put(si.getId(), si);
		}
	}*/

	private void readTopology() {
		neighborsMap = new HashMap<>();
		if (cluster.getTopology() != null) {
			for (Connect c : cluster.getTopology().getConnect()) {
				ArrayList<ServerNode> currentGroup = new ArrayList<>();
				ArrayList<String> currentGroupIds = new ArrayList<>();
				for (String id : c.getId()) {
					currentGroup.add(nodes.get(id));
					currentGroupIds.add(id);
				}
				for (String id : c.getId()) {
					for (ServerNode si : currentGroup) {
						if (si.id.equals(id))
							continue;
						if (!neighborsMap.containsKey(id))
							neighborsMap.put(id, new HashMap<>());
						neighborsMap.get(id).put(si.id, si);
					}
				}

			}
		}
	}

	public void writeServerConfigFiles() throws JAXBException {
		writeServerConfigFiles(cluster.getName());
	}

	public void writeServerConfigFiles(String destinationFolder) throws JAXBException {
		for (ServerNode node : nodes.values()) {
			Config sc = new Config();
			sc.setId(node.id);
			sc.setClientPort(node.clientPort + "");
			sc.setServerPort(node.serverPort);
			sc.setControlPort(node.controlPort + "");

			if (node.storageClassName != null || node.storageProperties != null) {
				Storage storageConfig = new Storage();
				storageConfig.setClassName(node.storageClassName);
				storageConfig.getProperty().addAll(node.storageProperties);
				sc.setStorage(storageConfig);
			}

			sc.setConnectorSleepTime(node.connectorSleepTime);
			sc.setChannelCapacity(node.channelCapacity);
			sc.setProtocolLogFile(node.protocolLogFile);
			sc.setProtocolLogLevel(node.protocolLogLevel);
			sc.setProtocolLogType(node.protocolLogType);
			sc.setProtocolStdLogLevel(node.protocolStdLogLevel);
			sc.setFrameworkLogFile(node.frameworkLogFile);
			sc.setFrameworkLogLevel(node.frameworkLogLevel);
			sc.setFrameworkLogType(node.frameworkLogType);
			sc.setFrameworkStdLogLevel(node.frameworkStdLogLevel);

			ConnectTo neighbors = null;
			if (neighborsMap != null && neighborsMap.get(node.id) != null && neighborsMap.get(node.id).values().size() > 0) {
				neighbors = new ConnectTo();
				for (ServerNode neighborNode : neighborsMap.get(node.id).values()) {
					ServerInfo si = new ServerInfo();
					si.setId(neighborNode.id);
					si.setIp(neighborNode.ip);
					si.setPort(neighborNode.serverPort);
					neighbors.getServer().add(si);
				}
			}
			if (neighbors != null)
				sc.setConnectTo(neighbors);

			if (node.protocolProperties != null) {
				sc.setProtocolProperties(new ProtocolProperties());
				sc.getProtocolProperties().getProperty().addAll(node.protocolProperties);
			}
			String outputFile = String.format("%s/server_%s", destinationFolder, node.id);
			Utils.checkParentAndCreate(outputFile);
			try {
				ConfigWriter.writeConfig(sc, new FileOutputStream(outputFile, false));
			} catch (FileNotFoundException e) {
				// should't happen because we created directory
				e.printStackTrace();
			}
		}
	}

	public String writeClientConfigFiles(String serverId, String stdLogLevel, String destinationFolder) throws JAXBException, FileNotFoundException {
		ServerNode node = nodes.get(serverId);
		Config sc = new Config();
		if (node.clientStorageClassName != null || node.clientStorageProperties != null) {
			Storage sConfig = new Storage();
			sConfig.setClassName(node.clientStorageClassName);
			sConfig.getProperty().addAll(node.clientStorageProperties);
			sc.setStorage(sConfig);
		}

		ConnectTo neighbors = new ConnectTo();
		ServerInfo newSi = new ServerInfo();
		newSi.setId(node.id);
		newSi.setIp(node.ip);
		newSi.setPort(node.clientPort + "");
		neighbors.getServer().add(newSi);
		sc.setConnectTo(neighbors);

		String files = destinationFolder + "/" + cluster.name;
		sc.setFrameworkLogFile(files + "/logs/frameworkLog");
		sc.setProtocolLogFile(files + "/logs/protocolLog");
		sc.setFrameworkStdLogLevel(stdLogLevel);
		sc.setProtocolStdLogLevel(stdLogLevel);

		// add protocol_properties
		sc.setProtocolProperties(new ProtocolProperties());
		sc.getProtocolProperties().getProperty().addAll(node.clientProtocolProperties);
		String outputFile = String.format("%s/server_%s", destinationFolder, serverId);
		Utils.checkParentAndCreate(outputFile);
		ConfigWriter.writeConfig(sc, new FileOutputStream(outputFile, false));
		return outputFile;
	}

	List<edu.msu.cse.dkvf.clusterManager.config.Property> setProperties(List<Property> clusterProperties, List<Property> serverProperties) {
		if (clusterProperties == null && serverProperties == null)
			return null;
		HashMap<String, edu.msu.cse.dkvf.clusterManager.config.Property> propertiesMap = new HashMap<>();
		if (serverProperties != null) {
			for (Property p : serverProperties) {
				edu.msu.cse.dkvf.clusterManager.config.Property newP = new edu.msu.cse.dkvf.clusterManager.config.Property();
				newP.setKey(p.getKey());
				newP.setValue(p.getValue());
				propertiesMap.put(p.getKey(), newP);
			}
		}
		if (clusterProperties != null) {
			for (Property p : clusterProperties) {
				if (!propertiesMap.containsKey(p.getKey())) {
					edu.msu.cse.dkvf.clusterManager.config.Property newP = new edu.msu.cse.dkvf.clusterManager.config.Property();
					newP.setKey(p.getKey());
					newP.setValue(p.getValue());
					propertiesMap.put(p.getKey(), newP);
				}
			}
		}
		return new ArrayList<edu.msu.cse.dkvf.clusterManager.config.Property>(propertiesMap.values());
	}

	String setProperty(String clusterValue, String serverValue) {
		if (serverValue != null)
			return serverValue.trim();
		else if (clusterValue != null)
			return clusterValue.trim();
		else
			return null;
	}

	String setProperty(String defaultStr, String clusterValue, String serverValue) {
		if (serverValue != null)
			return serverValue;
		if (clusterValue != null)
			return clusterValue;
		else
			return defaultStr;
	}

	public static void writeCluster(Cluster cluster, OutputStream out) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance(Cluster.class);

		Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
		jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
		
		jaxbMarshaller.marshal(cluster, out);
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
