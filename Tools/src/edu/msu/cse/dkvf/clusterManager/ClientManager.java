package edu.msu.cse.dkvf.clusterManager;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import javax.xml.bind.JAXBException;

import org.xml.sax.SAXException;

import edu.msu.cse.dkvf.DKVFClient;
import edu.msu.cse.dkvf.config.ConfigReader;

public class ClientManager {
	DKVFClient client;
	String errorMessage;

	public ClientManager(String clientClassName, String configFile) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, SAXException, JAXBException {
		ConfigReader cnfReader = null;
		cnfReader = new ConfigReader(configFile);
		Class<?> clientClass = Class.forName(clientClassName.trim());
		Constructor<?> clientConstructor = clientClass.getConstructor(ConfigReader.class);
		client = (DKVFClient) clientConstructor.newInstance(cnfReader);
		client.runAll();
	}

	public boolean put(String key, byte[] value) {
		try {
			return client.put(key, value);
		} catch (Exception e) {
			setErrorMessage(e);
			return false;
		}
	}

	public byte[] get(String key) {
		try {
			return client.get(key);
		} catch (Exception e) {
			setErrorMessage(e);
			return null;
		}
	}


	private void setErrorMessage(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		setErrorMessage(sw.toString());
	}

	private void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	

	public String getErrorMessage() {
		return errorMessage;
	}
	
	public void close (){
		client.prepareToTurnOff();
	}
}
