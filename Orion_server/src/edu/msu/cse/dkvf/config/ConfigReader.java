package edu.msu.cse.dkvf.config;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;


/**
 * Class to read XML configuration files.
 */
public class ConfigReader {

    /**
     * Information of a server
     */
    public class ServerInfo {
        public String id;
        public String ip;
        public int port;

        public String toString() {
            return "id: " + id + " ip: " + ip + " port: " + port;
        }

        public ServerInfo(ServerInfo that) {
            this.id = that.id;
            this.ip = that.ip;
            this.port = that.port;
        }

        public ServerInfo() {

        }

        public ServerInfo(String id, String ip, int port) {
            this.id = id;
            this.ip = ip;
            this.port = port;
        }
    }

    Config cnf;
    HashMap<String, List<String>> protocolProperties;
    HashMap<String, String> storageProperties;
    ArrayList<ServerInfo> serverInfos;

    // Default values:
    final static String SERVER_PORT_DEFAULT = "2000";
    final static String CONTROL_PORT_DEFAULT = "2001";
    final static String CLIENT_PORT_DEFAULT = "2002";
    final static String INSTANT_STABLE_DEFAULT = "false";
    final static String MULTI_VERSION_DEFAULT = "true";
    final static String STORAGE_CLASS_NAME_DEFAULT = "edu.msu.cse.dkvf.bdbStorage.BDBStorage";
    final static String DB_NAME_DEFAULT = "MyDB";
    final static String DB_DIRECTORY_DEFAULT = "DB";
    final static String CONNECTOR_SLEEP_TIME_DEFAULT = "10";
    final static String CHANNEL_CAPACITY_DEFAULT = Integer.MAX_VALUE + "";
    final static boolean SYNCH_COMMUNICATION_DEFAULT = false;
    final static String PROTOCOL_LOG_FILE_DEFAULT = "logs/protocol_log";
    final static String PROTOCOL_LOG_LEVEL_DEFAULT = "severe";
    final static String PROTOCOL_LOG_TYPE_DEFAULT = "text";
    final static String PROTOCOL_STD_LOG_LEVEL_DEFAULT = "off";
    final static String FRAMEWORK_LOG_FILE_DEFAULT = "logs/framework_log";
    final static String FRAMEWORK_LOG_LEVEL_DEFAULT = "severe";
    final static String FRAMEWORK_LOG_TYPE_DEFAULT = "text";
    final static String FRAMEWORK_STD_LOG_LEVEL_DEFAULT = "off";
    final static String XSD_FILE = "XSDs/Config.xsd";

    /**
     * Constructor for ConfigReader
     *
     * @param configFile The configuration file
     * @throws IllegalArgumentException
     */
    public ConfigReader(String configFile) throws IllegalArgumentException {
        try {
            SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

            InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(XSD_FILE);
            Schema schema = schemaFactory.newSchema(new StreamSource(in));

            File file = new File(configFile);
            JAXBContext jaxbContext = JAXBContext.newInstance(Config.class);

            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            jaxbUnmarshaller.setSchema(schema);

            setConfig((Config) jaxbUnmarshaller.unmarshal(file));
        } catch (Exception e) {
            throw new IllegalArgumentException("Bad configuration file", e);
        }
    }

    /**
     * Reads the {@link Config} object to explore its elements.
     *
     * @param cnf Config object to read
     */
    private void setConfig(Config cnf) {
        this.cnf = cnf;
        if (cnf.getClientPort() == null)
            cnf.setClientPort(CLIENT_PORT_DEFAULT);
        if (cnf.getServerPort() == null)
            cnf.setServerPort(SERVER_PORT_DEFAULT);
        if (cnf.getControlPort() == null)
            cnf.setControlPort(CONTROL_PORT_DEFAULT);
        if (cnf.getConnectorSleepTime() == null)
            cnf.setConnectorSleepTime(CONNECTOR_SLEEP_TIME_DEFAULT);
        if (cnf.getChannelCapacity() == null)
            cnf.setChannelCapacity(CHANNEL_CAPACITY_DEFAULT);
        if (cnf.isSynchCommunication() == null)
            cnf.setSynchCommunication(SYNCH_COMMUNICATION_DEFAULT);
        if (cnf.getFrameworkLogFile() == null)
            cnf.setFrameworkLogFile(FRAMEWORK_LOG_FILE_DEFAULT);
        if (cnf.getFrameworkLogLevel() == null)
            cnf.setFrameworkLogLevel(FRAMEWORK_LOG_LEVEL_DEFAULT);
        if (cnf.getFrameworkLogType() == null)
            cnf.setFrameworkLogType(FRAMEWORK_LOG_TYPE_DEFAULT);
        if (cnf.getFrameworkStdLogLevel() == null)
            cnf.setFrameworkStdLogLevel(FRAMEWORK_STD_LOG_LEVEL_DEFAULT);
        if (cnf.getProtocolLogFile() == null)
            cnf.setProtocolLogFile(PROTOCOL_LOG_FILE_DEFAULT);
        if (cnf.getProtocolLogLevel() == null)
            cnf.setProtocolLogLevel(PROTOCOL_LOG_LEVEL_DEFAULT);
        if (cnf.getProtocolLogType() == null)
            cnf.setProtocolLogType(PROTOCOL_LOG_TYPE_DEFAULT);
        if (cnf.getProtocolStdLogLevel() == null)
            cnf.setProtocolStdLogLevel(PROTOCOL_STD_LOG_LEVEL_DEFAULT);
        if (cnf.getStorage() == null) {
            cnf.setStorage(new Storage());

            Property p1 = new Property();
            p1.setKey("db_directory");
            p1.setValue(DB_DIRECTORY_DEFAULT);

            Property p2 = new Property();
            p2.setKey("db_name");
            p2.setValue(DB_NAME_DEFAULT);

            Property p3 = new Property();
            p3.setKey("instant_stable");
            p3.setValue(INSTANT_STABLE_DEFAULT);

            Property p4 = new Property();
            p4.setKey("multi_version");
            p4.setValue(MULTI_VERSION_DEFAULT);

            cnf.getStorage().getProperty().add(p1);
            cnf.getStorage().getProperty().add(p2);
            cnf.getStorage().getProperty().add(p3);
            cnf.getStorage().getProperty().add(p4);
        }
        if (cnf.getStorage().getClassName() == null)
            cnf.getStorage().setClassName(STORAGE_CLASS_NAME_DEFAULT);

        readStorageProperties();
        readProtocolProperties();
        readServerInfos();
    }

    /**
     * Reads storage related properties.
     */
    private void readStorageProperties() {
        storageProperties = new HashMap<>();
        for (Property p : cnf.getStorage().getProperty()) {
            storageProperties.put(p.getKey().trim(), p.getValue().trim());
        }
    }

    /**
     * Reads protocol related properties.
     */
    private void readProtocolProperties() {
        protocolProperties = new HashMap<>();
        if (cnf.getProtocolProperties() != null) {
            for (Property p : cnf.getProtocolProperties().getProperty()) {
                ArrayList<String> value = new ArrayList<>();
                String valueStr = p.getValue();
                String[] valueStrParts = valueStr.trim().split(",");
                for (String s : valueStrParts) {
                    s = s.trim();
                    if (!s.isEmpty())
                        value.add(s.trim());
                }
                protocolProperties.put(p.getKey().trim(), value);
            }
        }
    }

    /**
     * Reads servers information.
     */
    private void readServerInfos() {
        serverInfos = new ArrayList<>();
        if (cnf.getConnectTo() != null) {
            for (edu.msu.cse.dkvf.config.ServerInfo si : cnf.getConnectTo().getServer()) {
                ServerInfo newSi = new ServerInfo();
                newSi.id = si.getId().trim();
                newSi.ip = si.getIp().trim();
                newSi.port = new Integer(si.getPort().trim());

                serverInfos.add(newSi);
            }
        }
    }

    /**
     * Gets the parsed configuration.
     *
     * @return The parsed configuration.
     */
    public Config getConfig() {
        return cnf;
    }

    /**
     * Gets the parsed storage properties.
     *
     * @return The parsed storage properties
     */
    public HashMap<String, String> getStorageProperties() {
        return storageProperties;
    }

    /**
     * Gets the parsed protocol properties.
     *
     * @return The parsed protocol properties
     */
    public HashMap<String, List<String>> getProtocolProperties() {
        return protocolProperties;
    }

    /**
     * Gets the parsed server information.
     *
     * @return The parsed server information
     */
    public ArrayList<ServerInfo> getServerInfos() {
        return serverInfos;
    }
}
