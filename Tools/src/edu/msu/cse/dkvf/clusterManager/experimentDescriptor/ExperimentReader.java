package edu.msu.cse.dkvf.clusterManager.experimentDescriptor;

import java.io.File;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import com.jcraft.jsch.JSchException;

import edu.msu.cse.dkvf.clusterManager.SSHManager;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Parameters;

public class ExperimentReader {

	public class YcsbClientNode {
		public String id;
		public String ip;
		public String userName;
		public String password;
		public String key;
		public String jarFileAddress;
		public String jarFileName;
		public String workloadAddress;
		public String workloadName;
		public String clientClassName;
		public String workingDirectory;
		public String ycsbDriverName;
		public String os;
		public SSHManager sshManager;

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

	Experiment experiment;
	public HashMap<String, YcsbClientNode> nodes;
	public ArrayList<String> variationNames;

	final static String OS_DEFAULT = "linux";
	final static String YCSB_DRIVER_NAEM_DEFAULT = "edu.msu.cse.dkvf.ycsbDriver.DKVFDriver";
	final static String USERNAME_DEFAULT = "ubuntu";
	final static String EXPERIMENT_XSD_FILE = "XSDs/ExperimentDescriptor.xsd";

	public ExperimentReader(String fileAddress) throws JAXBException, SAXException {
		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

		InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(EXPERIMENT_XSD_FILE);
		Schema schema = schemaFactory.newSchema(new StreamSource(in));

		File file = new File(fileAddress);
		JAXBContext jaxbContext = JAXBContext.newInstance(Experiment.class);

		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		jaxbUnmarshaller.setSchema(schema);

		setExperiment((Experiment) jaxbUnmarshaller.unmarshal(file));

	}

	public void setExperiment(Experiment experiment) {
		this.experiment = experiment;
		if (experiment.getDefaults() == null)
			experiment.setDefaults(new edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Parameters());
		readNodes();
		readVariations();
	}

	private void readVariations() {
		variationNames = new ArrayList<>();
		if (experiment.getWorkloadVariations() == null)
			return;
		for (Variation v : experiment.getWorkloadVariations().getVariation()) {
			variationNames.add(v.getName().trim());
		}
	}

	private void readNodes() {
		nodes = new HashMap<>();
		for (Client client : experiment.getClients().getClient()) {
			if (client.getConfig() == null)
				client.setConfig(new Parameters());
			YcsbClientNode node = new YcsbClientNode();
			node.id = client.getId();
			node.userName = setProperty(USERNAME_DEFAULT, experiment.getDefaults().getUsername(), client.getConfig().getUsername());
			node.os = setProperty(OS_DEFAULT, experiment.getDefaults().getOs(), client.getConfig().getOs());
			node.ycsbDriverName = setProperty(YCSB_DRIVER_NAEM_DEFAULT, experiment.getDefaults().getYcsbDriver(), client.getConfig().getYcsbDriver());
			node.clientClassName = setProperty(experiment.getDefaults().getClientClassName(), client.getConfig().getClientClassName());
			if (node.clientClassName == null)
				throw new RuntimeException("You have to provide client_class_name!");
			node.password = setProperty(experiment.getDefaults().getPassword(), client.getConfig().getPassword());
			node.key = setProperty(experiment.getDefaults().getKey(), client.getConfig().getKey());
			node.ip = client.getIp();
			node.jarFileAddress = setProperty(experiment.getDefaults().getClientJarFile(), client.getConfig().getClientJarFile());
			if (node.jarFileAddress == null)
				throw new RuntimeException("You have to provide client_jar_file!");
			node.jarFileName = node.jarFileAddress.substring(node.jarFileAddress.lastIndexOf("/") + 1);

			node.workloadAddress = setProperty(experiment.getDefaults().getWorkload(), client.getConfig().getWorkload());
			if (node.workloadAddress == null)
				throw new RuntimeException("You have to provide workload!");
			node.workloadName = node.workloadAddress.substring(node.workloadAddress.lastIndexOf("/") + 1);

			node.workingDirectory = setProperty(experiment.getDefaults().getWorkingDirectory(), client.getConfig().getWorkingDirectory());
			if (node.workingDirectory == null)
				throw new RuntimeException("You have to provide working_directory!");
			else
				node.workingDirectory = node.workingDirectory.trim();
			nodes.put(node.id, node);
		}
	}

	public Experiment getExperiment() {
		return experiment;
	}

	String setProperty(String defaultStr, String clusterValue, String serverValue) {
		if (serverValue != null)
			return serverValue;
		if (clusterValue != null)
			return clusterValue;
		else
			return defaultStr;
	}

	String setProperty(String clusterValue, String serverValue) {
		if (serverValue != null)
			return serverValue;
		else
			return clusterValue;
	}
}
