package edu.msu.cse.dkvf;



import java.text.MessageFormat;

import edu.msu.cse.dkvf.ServerConnector.NetworkStatus;
import edu.msu.cse.dkvf.Storage.StorageStatus;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.ClientMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;

/**
 * The base class for the client side of the protocol. Any protocol needs to
 * extends this class for the client side.
 *
 */
public abstract class DKVFClient extends DKVFBase {

	/**
	 * The abstract method for putting a value with the given key.
	 * 
	 * @param key
	 *            The key of data to put
	 * @param value
	 *            The value to put
	 * @return The result of the operation. <br/>
	 *         <b>true</b> successful  <br/>
	 *         <b>false</b> unsuccessful
	 */
	public abstract boolean put(String key, byte[] value);

	/**
	 * The abstract method for getting a value with the given key.
	 * 
	 * @param key
	 *            The key of data to put
	 * @return The value resulted for the give key. <br/>
	 *         <b>null</b> if the key does not exist.
	 */
	public abstract byte[] get(String key);

	public DKVFClient(ConfigReader cnfReader) {
		super(cnfReader);
	}

	/**
	 * Runs server connector and storage.
	 * 
	 * @return <b>true</b> if successful <br/>
	 *         <b>false</b> if unsuccessful
	 */
	public boolean runAll() {
		if (connectToServers() == NetworkStatus.SUCCESS){
			frameworkLOGGER.info("Sucessfully ran the server connector");
			
		}
		else
			return false;
		/* I comment here, for now. Later we need to add storage enable/disable in the XMLs
		 * and disable it for YCSB experiments, because client threads share same folder that 
		 * creates problems for lock. 
		
		
		if (runDb() == StorageStatus.SUCCESS)
			frameworkLOGGER.info("Sucessfully ran the stable storage");
		else
			return false;
			*/
		
		/*
		//debug
		try {
			
			Thread.sleep(15000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		return true;
		
	}

	/**
	 * Sends a client message to the server with the given ID.
	 * 
	 * @param serverId
	 *            The ID of the destination server.
	 * @param cm
	 *            The client message to send
	 * @return The result of the operation
	 */
	public NetworkStatus sendToServer(String serverId, ClientMessage cm) {
		try {
			serversOut.get(serverId).writeInt32NoTag(cm.getSerializedSize());
			cm.writeTo(serversOut.get(serverId));
			serversOut.get(serverId).flush();
			//cm.writeDelimitedTo(serversOut.get(serverId));
			frameworkLOGGER.finer(MessageFormat.format("Sent to server with id= {0} \n{1}", serverId, cm.toString()));
			return NetworkStatus.SUCCESS;
		} catch (Exception e) {
			frameworkLOGGER.warning(Utils.exceptionLogMessge(MessageFormat.format("Problem in sending to server with Id= {0}" , serverId), e));
			return NetworkStatus.FAILURE;
		}
	}

	/**
	 * Reads from input stream of the server with the given ID.
	 * 
	 * @param serverId
	 *            The ID of the server to read from its input stream.
	 * @return The received ClientReply message.
	 */
	public ClientReply readFromServer(String serverId) {
		try {
			int size = serversIn.get(serverId).readInt32();
			byte[] result = serversIn.get(serverId).readRawBytes(size);
			return ClientReply.parseFrom(result);
		} catch (Exception e) {
			//debug
			frameworkLOGGER.warning("serversIn= " + serverId + " serversIn.get(serverId) = " + serversIn.get(serverId) + " serversOut.get(serverId)= " + serversOut.get(serverId));
			frameworkLOGGER.warning(Utils.exceptionLogMessge(MessageFormat.format("Problem in reading response from server with Id= {0}" , serverId), e));
			return null;
		}
	}

}
