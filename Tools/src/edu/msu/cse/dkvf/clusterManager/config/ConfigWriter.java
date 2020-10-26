package edu.msu.cse.dkvf.clusterManager.config;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import edu.msu.cse.dkvf.clusterManager.config.Config;

public class ConfigWriter {
	public static void writeConfig(Config serverConfig, OutputStream out) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance(Config.class);

		Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
		jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
		
		jaxbMarshaller.marshal(serverConfig, out);
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
