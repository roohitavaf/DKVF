package edu.msu.cse.dkvf;


import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import edu.msu.cse.dkvf.ServerConnector.NetworkStatus;
import edu.msu.cse.dkvf.Storage.StorageStatus;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.config.ConfigReader.ServerInfo;
import edu.msu.cse.dkvf.metadata.Metadata.*;
import edu.msu.cse.dkvf.controlMetadata.ControlMetadata.ControlMessage;
import edu.msu.cse.dkvf.controlMetadata.ControlMetadata.ControlReply;

/**
 * The base class for the server side of the protocol. Any protocol needs to
 * extends this class for the server side.
 *
 */
public abstract class DKVFServer extends DKVFBase {
	/**
	 * Number of clients connected to this server.
	 */
	AtomicInteger numOfClients = new AtomicInteger(0);

	/**
	 * Number of servers connected to this servers. Failed servers are excluded.
	 */
	AtomicInteger numOfServers = new AtomicInteger(0);

	/**
	 * ID of this server.
	 */
	String id;
	
	
	boolean synchCommunication = false;
	
	Map<String, ChannelManager> channelManagers = new HashMap<>();

	/**
	 * Constructor for DKVFServer  
	 * @param cnfReader The configuration reader
	 */
	public DKVFServer(ConfigReader cnfReader) {
		super(cnfReader);
		id = cnfReader.getConfig().getId();
		synchCommunication = cnfReader.getConfig().isSynchCommunication();
	}

	/**
	 * Runs all service including server connector, storage, server and client
	 * listeners.
	 * 
	 * @return <b>true</b> if successful <br/>
	 * 
	 *         <b>false</b> if unsuccessful
	 */
	public boolean runAll() {
		//setting up synchronous communication if it is requested in the config file.
		if (synchCommunication){
			if ( connectToServers() == NetworkStatus.SUCCESS)
				frameworkLOGGER.info("Sucessfully ran the server connector");
			else {
				frameworkLOGGER.info("Failed to setup up synchronous communication.");
				return false;
			}
		}
		
		//asynchronous channels are always enalbled.
		if (setupChannelManagers() == NetworkStatus.SUCCESS)
			frameworkLOGGER.info("Sucessfully ran server channel managers.");
		else {
			frameworkLOGGER.info("Failed to setup up channels for asynchronous communication.");
			return false;
		}
		if (runDb() == StorageStatus.SUCCESS)
			frameworkLOGGER.info("Sucessfully ran the stable storage");
		else {
			frameworkLOGGER.info("Failed to setup up storage.");
			return false;
		}
		if (runListeners() == NetworkStatus.FAILURE){
			frameworkLOGGER.info("Failed to setup listeners.");
			return false;
		}

		return true;

	}

	/**
	 * Sets up {@link ChannelManagers} for peer servers.
	 * @return The result of the operation
	 */
	private NetworkStatus setupChannelManagers() {
		try {
			for (ServerInfo si : cnfReader.getServerInfos()) {
				if (!channelManagers.containsKey(si.id)) {
					ChannelManager cm = new ChannelManager(si.ip, si.port, new Integer(cnf.getConnectorSleepTime().trim()), new Integer(cnf.getChannelCapacity().trim()),frameworkLOGGER);
					channelManagers.put(si.id, cm);
				}
			}

			return NetworkStatus.SUCCESS;
		} catch (Exception e) {
			frameworkLOGGER.severe(MessageFormat.format("Problem in creating channel managers. \n {0}", e.toString()));
			return NetworkStatus.FAILURE;
		}
	}

	/**
	 * Runs listeners for servers and clients.
	 * 
	 * @return The result of the operation
	 */
	public NetworkStatus runListeners() {
		if (runServerListener() == NetworkStatus.SUCCESS)
			frameworkLOGGER.info("Sucessfully run the server listener");
		else
			return NetworkStatus.FAILURE;
			
		if (runClientListener() == NetworkStatus.SUCCESS)
			frameworkLOGGER.info("Sucessfully run the client listener");
		else
			return NetworkStatus.FAILURE;
		if (runControlListener() == NetworkStatus.SUCCESS)
			frameworkLOGGER.info("Sucessfully run the control listener");
		else
			return NetworkStatus.FAILURE;

		return NetworkStatus.SUCCESS;

	}

	/**
	 * Sends a server message to the server with the given ID. 
	 * It guarantees that all messages are delivered. 
	 * It also guarantees FIFO delivery.
	 * It does not wait to actually send the message. Instead, it immediately returns.  
	 * However, each channel has a capacity defined by configuration file 
	 * that if reached, no more message can be added to the channel, and protocol designer must deal with it.  
	 * 
	 * @param serverId
	 *            The ID of the destination server.
	 * @param sm
	 *            The server message to send
	 * @return The result of the operation
	 */
	public NetworkStatus sendToServerViaChannel(String serverId, ServerMessage sm) {
		if (channelManagers.containsKey(serverId)) {
			channelManagers.get(serverId).addMessage(sm);
			return NetworkStatus.SUCCESS;
		} else {
			frameworkLOGGER.severe(MessageFormat.format("No server found for id= {0}", serverId));
			return NetworkStatus.FAILURE;
		}
	}
	
	
	/**
	 * Sends a server message to the server with the given ID. 
	 * It does not guarantees delivery. It should be used for messages that tolerate loss.
	 * @param serverId
	 * 			The ID of the destination server. 
	 * @param cm
	 * 			The client message to send
	 * @return The result of the operation
	 */
	public NetworkStatus sendToServer(String serverId, ServerMessage sm) {
		if (serversOut.containsKey(serverId)) {
			try {
				synchronized (serversOut.get(serverId)) {
					serversOut.get(serverId).writeInt32NoTag(sm.getSerializedSize());
					sm.writeTo(serversOut.get(serverId));
					serversOut.get(serverId).flush();
					//sm.writeDelimitedTo(serversOut.get(serverId));
					frameworkLOGGER.finest(MessageFormat.format("Sent to server with id= {0} \n{1}", serverId, sm.toString()));
					return NetworkStatus.SUCCESS;
				}
			} catch (IOException e) {
				serversIn.remove(serverId);
				serversOut.remove(serverId);
				connectToServers();
				frameworkLOGGER.severe(MessageFormat.format("Problem in sending to server with id= {0}, Message:\n{1}",
						serverId, e.getMessage()));
				return NetworkStatus.FAILURE;
			}
		} else {
			frameworkLOGGER.severe(
					MessageFormat.format("No server found for id= {0} for message: \n {1}", serverId, sm.toString()));
			return NetworkStatus.FAILURE;
		}
	}
	
	/**
	 * Reads from input stream of the server with the given ID.
	 * 
	 * @param serverId
	 *            The ID of the server to read from its input stream.
	 * @return The received ServerMessage object.
	 */
	public ServerMessage readFromServer(String serverId) {
		if (serversIn.containsKey(serverId)) {
			try {
				ServerMessage sm;
				synchronized (serversIn.get(serverId)) {
					int size = serversIn.get(serverId).readInt32();
					byte[] newMessageBytes = serversIn.get(serverId).readRawBytes(size);
					sm = ServerMessage.parseFrom(newMessageBytes);
					//sm = ServerMessage.parseDelimitedFrom(serversIn.get(serverId));
					frameworkLOGGER.finer(MessageFormat.format("Read from server with id={0} \n{1}", serverId, sm.toString()));
				}
				return sm;

			} catch (Exception e) {
				frameworkLOGGER.severe(MessageFormat.format("Problem in reading from server with id= {0}, toString= {1}, Message= {2}", serverId, e.toString(), e.getMessage()));
				return null;
			}
		} else {
			frameworkLOGGER.severe(MessageFormat.format("No server found for id= {0}", serverId));
			return null;
		}
	}
	/**
	 * Runs the control listener thread.
	 * 
	 * @return The result of the operation.
	 */
	private NetworkStatus runControlListener() {
		try {
			ControlListenerHandler sl = new ControlListenerHandler(new Integer(cnf.getControlPort().trim()), this, frameworkLOGGER);
			Thread t = new Thread(sl);
			t.start();
			return NetworkStatus.SUCCESS;
		} catch (Exception e) {
			frameworkLOGGER.severe(MessageFormat.format("Failed to run Control Listener at port= {0}, toString={1}, Message={2}", cnf.getControlPort(), e.toString(), e.getMessage()));
			return NetworkStatus.FAILURE;
		}
	}
	
	/**
	 * Runs listener for incoming server messages
	 * @return The result of the operation
	 */
	private NetworkStatus runServerListener() {
		try {
			ServerListener sl = new ServerListener(new Integer(cnf.getServerPort().trim()), this, frameworkLOGGER);
			Thread t = new Thread(sl);
			t.start();
			return NetworkStatus.SUCCESS;
		} catch (Exception e) {
			frameworkLOGGER.severe(MessageFormat.format("Failed to run Servers Listener at port= {0}, toString={1}, Message={2}", cnf.getServerPort(), e.toString(), e.getMessage()));
			return NetworkStatus.FAILURE;
		}
	}

	/**
	 * Runs the listener for clients.
	 * 
	 * @return The result of the operation.
	 */
	private NetworkStatus runClientListener() {
		try {
			ClientListener cl = new ClientListener(new Integer(cnf.getClientPort().trim()), this, frameworkLOGGER);
			Thread t = new Thread(cl);
			t.start();
			return NetworkStatus.SUCCESS;
		} catch (Exception e) {
			frameworkLOGGER.severe(MessageFormat.format("Failed to run Clients Listener at port= {0}, toString={1}, Message={2}", cnf.getClientPort(), e.toString(), e.getMessage()));
			return NetworkStatus.FAILURE;
		}
	}

	/**
	 * This message will be called for any incoming client message. Any protocol
	 * needs to implement this method.
	 * 
	 * @param cma
	 *            The received client message
	 */
	public abstract void handleClientMessage(ClientMessageAgent cma);

	/**
	 * This message will be called for any incoming server message. Any protocol
	 * needs to implement this method.
	 * 
	 * @param sm
	 *            The received server message
	 */
	public abstract void handleServerMessage(ServerMessage sm);

	/**
	 * This method will be called for any unknown control message. A protocol
	 * can override this method to use control channel.
	 * 
	 * @param cm
	 *            The received control message.
	 * @return The reply to the control message.
	 */
	public ControlReply handleControlMessage(ControlMessage cm) {
		return null;
	}

	/**
	 * Increments the number of clients counter.
	 */
	public void incrementNumberOfClients() {
		numOfClients.incrementAndGet();
	}

	/**
	 * Decrements the number of clients counter.
	 */
	public void decrementNumberOfClients() {
		numOfClients.decrementAndGet();
	}

	/**
	 * Gets the number of clients counter value.
	 * 
	 * @return Number of clients connected to this server
	 */
	public int getNumberOfClients() {
		return numOfClients.intValue();
	}

	/**
	 * Increments the number of servers counter.
	 */
	public void incrementNumberOfServers() {
		numOfServers.incrementAndGet();
	}

	/**
	 * Decrements the number of servers counter.
	 */
	public void decrementNumberOfServers() {
		numOfServers.decrementAndGet();
	}

	/**
	 * Gets the number of servers counter value.
	 * 
	 * @return Number of servers connected to this server
	 */
	public int getNumberOfServers() {
		return numOfServers.intValue();
	}

	/**
	 * Gets the server ID.
	 * 
	 * @return The ID of the server
	 */
	public String getId() {
		return id;
	}

	/**
	 * Gets the number of servers that are expected this server to connect
	 * according to the configuration.
	 * 
	 * @return The number of expected servers to connect
	 */
	public int getNumOfExpectedServers() {
		if (cnfReader.getConfig().getConnectTo() == null || cnfReader.getConfig().getConnectTo().getServer()== null)
			return 0;
		return cnfReader.getConfig().getConnectTo().getServer().size();
	}

}
