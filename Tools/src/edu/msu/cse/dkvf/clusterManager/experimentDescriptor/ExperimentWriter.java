package edu.msu.cse.dkvf.clusterManager.experimentDescriptor;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import edu.msu.cse.dkvf.clusterManager.ExperimentManager;
import edu.msu.cse.dkvf.clusterManager.Utils;
import edu.msu.cse.dkvf.clusterManager.config.Config;
import edu.msu.cse.dkvf.clusterManager.config.Config.ProtocolProperties;
import edu.msu.cse.dkvf.clusterManager.config.ConfigWriter;
import edu.msu.cse.dkvf.clusterManager.config.ConnectTo;
import edu.msu.cse.dkvf.clusterManager.config.ServerInfo;
import edu.msu.cse.dkvf.clusterManager.config.Storage;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ExperimentReader.YcsbClientNode;

public class ExperimentWriter {

	public final static String EXP_CLIENT_NAME_FORMAT = "ycsb_client_{0}";

	public final static String BAT_NAME_FORMAT_T = "runClient_{0}_{1}_t.bat";
	public final static String BAT_NAME_FORMAT_LOAD = "runClient_{0}_{1}_load.bat";

	public final static String SH_NAME_FORMAT_T = "runClient_{0}_{1}_t.sh";
	public final static String SH_NAME_FORMAT_LOAD = "runClient_{0}_{1}_load.sh";

	public final static String YCSB_RESULT_FORMAT_T = ExperimentManager.REMOTE_EXP_RESULTS_DIR +"/client_{0}_{1}_t.result";
	public final static String YCSB_RESULT_FORMAT_LOAD = ExperimentManager.REMOTE_EXP_RESULTS_DIR +"/client_{0}_{1}_load.result";

	public final static String YCSB_ERROR_FORMAT_T = ExperimentManager.REMOTE_EXP_ERRORS_DIR +"/client_{0}_{1}_t.error";
	public final static String YCSB_ERROR_FORMAT_LOAD = ExperimentManager.REMOTE_EXP_ERRORS_DIR + "/client_{0}_{1}_load.error";
	public final static String REMOTE_CLIENT_CONFIG = "client_config";
	public final static String REMOTE_LIB_DIR = "libs";

	private Experiment experiment;
	private HashMap<String, YcsbClientNode> nodes;

	public ExperimentWriter(Experiment experiment, HashMap<String, YcsbClientNode> nodes) {
		this.experiment = experiment;
		this.nodes = nodes;
	}

	public static void writeExperiment(Experiment experiment, OutputStream out) throws JAXBException {

		JAXBContext jaxbContext = JAXBContext.newInstance(Experiment.class);

		Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
		jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

		jaxbMarshaller.marshal(experiment, out);
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void writeExpScriptFiles(String destinationFolder) throws FileNotFoundException, UnsupportedEncodingException {

		for (YcsbClientNode node : nodes.values()) {
			writeExpScriptFile(node, null, destinationFolder);
			if (experiment.getWorkloadVariations() != null) {
				for (Variation v : experiment.getWorkloadVariations().getVariation()) {
					writeExpScriptFile(node, v, destinationFolder);
				}
			}
		}
	}

	public void writeExpScriptFile(YcsbClientNode node, Variation v, String destinationDir) throws FileNotFoundException, UnsupportedEncodingException {
		String scriptForTransaction = null;
		String contentForTransaction = null;

		String scriptForLoading = null;
		String contentForLoading = null;

		String variationStr = variationToString(v);
		String variationName = ExperimentManager.ORIGINAL_VARIATION_NAME; 
		if (v != null){
			variationName = v.getName().trim();
		}
		if (node.os.equals("windows")) {
			scriptForTransaction = MessageFormat.format(BAT_NAME_FORMAT_T, node.id, variationName);
			String resultFileName_t = MessageFormat.format(YCSB_RESULT_FORMAT_T, node.id, variationName);
			String errorFileName_t = MessageFormat.format(YCSB_ERROR_FORMAT_T, node.id, variationName);
			contentForTransaction = createBatContent(REMOTE_LIB_DIR, "-t", node, variationStr, resultFileName_t, errorFileName_t);

			scriptForLoading = MessageFormat.format(BAT_NAME_FORMAT_LOAD, node.id, variationName);
			String resultFileName_load = MessageFormat.format(YCSB_RESULT_FORMAT_LOAD, node.id, variationName);
			String errorFileName_load = MessageFormat.format(YCSB_ERROR_FORMAT_LOAD, node.id, variationName);
			contentForLoading = createBatContent(REMOTE_LIB_DIR, "-load", node, variationStr, resultFileName_load, errorFileName_load);

		} else {
			scriptForTransaction = MessageFormat.format(SH_NAME_FORMAT_T, node.id, variationName);
			String resultFileName_t = MessageFormat.format(YCSB_RESULT_FORMAT_T, node.id, variationName);
			String errorFileName_t = MessageFormat.format(YCSB_ERROR_FORMAT_T, node.id, variationName);
			contentForTransaction = createBashContent(REMOTE_LIB_DIR, "-t", node, variationStr, resultFileName_t, errorFileName_t);

			scriptForLoading = MessageFormat.format(SH_NAME_FORMAT_LOAD, node.id, variationName);
			String resultFileName_load = MessageFormat.format(YCSB_RESULT_FORMAT_LOAD, node.id, variationName);
			String errorFileName_load = MessageFormat.format(YCSB_ERROR_FORMAT_LOAD, node.id, variationName);
			contentForLoading = createBashContent(REMOTE_LIB_DIR, "-load", node, variationStr, resultFileName_load, errorFileName_load);

		}
		Utils.createTextFile(contentForTransaction, destinationDir + "/" + scriptForTransaction);
		Utils.createTextFile(contentForLoading, destinationDir + "/" + scriptForLoading);
	}

	public void writeExpClientConfigFiles(String destinationFolder) throws JAXBException {
		for (Client client : experiment.getClients().getClient()) {

			Storage storageConfig = new Storage();
			String experimentClassName = null;
			String serverClassName = null;
			if (experiment.getDefaults().getStorage() != null)
				experimentClassName = experiment.getDefaults().getStorage().getClassName();
			if (client.getConfig().getStorage() != null)
				serverClassName = client.getConfig().getStorage().getClassName();
			storageConfig.setClassName(setProperty(experimentClassName, serverClassName));

			List<Property> experimentStorageProperties = null;
			List<Property> serverStorageProperties = null;
			if (experiment.getDefaults().getStorage() != null)
				experimentStorageProperties = experiment.getDefaults().getStorage().getProperty();
			if (client.getConfig().getStorage() != null)
				serverStorageProperties = client.getConfig().getStorage().getProperty();
			List<edu.msu.cse.dkvf.clusterManager.config.Property> storageProperties = setProperties(experimentStorageProperties, serverStorageProperties);

			for (edu.msu.cse.dkvf.clusterManager.config.Property p : storageProperties)
				storageConfig.getProperty().add(p);

			String connectorSleepTime = setProperty(experiment.getDefaults().getConnectorSleepTime(), client.getConfig().getConnectorSleepTime());

			String protocolLogFile = setProperty(experiment.getDefaults().getProtocolLogFile(), client.getConfig().getProtocolLogFile());

			String protocolLogLevel = setProperty(experiment.getDefaults().getProtocolLogLevel(), client.getConfig().getProtocolLogLevel());
			String protocolLogType = setProperty(experiment.getDefaults().getProtocolLogType(), client.getConfig().getProtocolLogType());
			String protocolStdLogLevel = setProperty(experiment.getDefaults().getProtocolStdLogLevel(), client.getConfig().getProtocolStdLogLevel());

			String frameworkLogFile = setProperty(experiment.getDefaults().getFrameworkLogFile(), client.getConfig().getFrameworkLogFile());
			String frameworkLogLevel = setProperty(experiment.getDefaults().getFrameworkLogLevel(), client.getConfig().getFrameworkLogLevel());
			String frameworkLogType = setProperty(experiment.getDefaults().getFrameworkLogType(), client.getConfig().getFrameworkLogType());
			String frameworkStdLogLevel = setProperty(experiment.getDefaults().getFrameworkStdLogLevel(), client.getConfig().getFrameworkStdLogLevel());

			List<Property> clusterProtocolProperties = null;
			List<Property> serverProtocolProperties = null;
			if (experiment.getDefaults().getProtocolProperties() != null)
				clusterProtocolProperties = experiment.getDefaults().getProtocolProperties().getProperty();
			if (client.getConfig().getProtocolProperties() != null)
				serverProtocolProperties = client.getConfig().getProtocolProperties().getProperty();
			List<edu.msu.cse.dkvf.clusterManager.config.Property> protocolProperties = setProperties(clusterProtocolProperties, serverProtocolProperties);
			
			

			ConnectTo connectTo = new ConnectTo();

			for (edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ServerInfo si : client.getConnectTo().getServer()) {
				ServerInfo newSi = new ServerInfo();
				newSi.setId(si.getId());
				newSi.setIp(si.getIp());
				newSi.setPort(si.getPort());

				connectTo.getServer().add(newSi);

			}

			Config sc = new Config();
			sc.setId(client.getId());
			if (storageConfig.getClassName() != null)
				sc.setStorage(storageConfig);
			sc.setConnectorSleepTime(connectorSleepTime);
			sc.setProtocolLogFile(protocolLogFile);
			sc.setProtocolLogLevel(protocolLogLevel);
			sc.setProtocolLogType(protocolLogType);
			sc.setProtocolStdLogLevel(protocolStdLogLevel);
			sc.setFrameworkLogFile(frameworkLogFile);
			sc.setFrameworkLogLevel(frameworkLogLevel);
			sc.setFrameworkLogType(frameworkLogType);
			sc.setFrameworkStdLogLevel(frameworkStdLogLevel);
			sc.setConnectTo(connectTo);
			sc.setProtocolProperties(new ProtocolProperties());
			sc.getProtocolProperties().getProperty().addAll(protocolProperties);
			String outputFile = MessageFormat.format(EXP_CLIENT_NAME_FORMAT, client.getId());
			Utils.checkParentAndCreate(destinationFolder + "/" + outputFile);
			try {
				ConfigWriter.writeConfig(sc, new FileOutputStream(destinationFolder + "/" + outputFile, false));
			} catch (FileNotFoundException e) {
				// should't happen because we created directory
				e.printStackTrace();
			}
		}
	}

	private String createBatContent(String libs, String operation, YcsbClientNode node, String variation, String resultFile, String errorFile) {
		return MessageFormat.format("java -cp {0}/*;{1} com.yahoo.ycsb.Client {2} -db {3} -P {4} -p clientClassName={5} -p clientConfigFile={6} {7} > {8} 2> {9}", libs.trim(), node.jarFileName.trim(), operation.trim(), node.ycsbDriverName.trim(), node.workloadName.trim(), node.clientClassName.trim(), REMOTE_CLIENT_CONFIG, variation.trim(), resultFile.trim(), errorFile.trim());
	}

	private String createBashContent(String libs, String operation, YcsbClientNode node, String variation, String resultFile, String errorFile) {
		return MessageFormat.format("#!/bin/sh \njava -cp {0}/*:{1} com.yahoo.ycsb.Client {2} -db {3} -P {4} -p clientClassName={5} -p clientConfigFile={6} {7} > {8} 2> {9}", libs.trim(), node.jarFileName.trim(), operation.trim(), node.ycsbDriverName.trim(), node.workloadName.trim(), node.clientClassName.trim(), REMOTE_CLIENT_CONFIG.trim(), variation.trim(), resultFile.trim(), errorFile.trim());
	}

	String setProperty(String clusterValue, String serverValue) {
		if (serverValue != null)
			return serverValue;
		else
			return clusterValue;
	}

	List<edu.msu.cse.dkvf.clusterManager.config.Property> setProperties(List<Property> clusterProperties, List<Property> serverProperties) {
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

	private String variationToString(Variation v) {
		if (v == null)
			return "";
		StringBuffer sb = new StringBuffer();
		for (Property p : v.getProperty()) {
			String key = p.getKey();
			String value = p.getValue();
			sb.append(" -p ").append(key).append("=").append(value).append(" ");
		}
		return sb.toString();
	}
}