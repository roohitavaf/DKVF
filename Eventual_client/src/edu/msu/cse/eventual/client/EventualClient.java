package edu.msu.cse.eventual.client;


import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;

import com.google.protobuf.ByteString;

import edu.msu.cse.dkvf.DKVFClient;
import edu.msu.cse.dkvf.Utils;
import edu.msu.cse.dkvf.ServerConnector.NetworkStatus;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.ClientMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;
import edu.msu.cse.dkvf.metadata.Metadata.GetMessage;
import edu.msu.cse.dkvf.metadata.Metadata.PutMessage;

public class EventualClient extends DKVFClient{
	ConfigReader cnfReader;
	
	int dcId;
	int numOfPartitions;
	
	public EventualClient(ConfigReader cnfReader) {
		super(cnfReader);
		this.cnfReader = cnfReader;
		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();
		numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));
		dcId = new Integer(protocolProperties.get("dc_id").get(0));
	}
	public boolean put(String key, byte[] value) {
		try {
			PutMessage pm = PutMessage.newBuilder().setKey(key).setValue(ByteString.copyFrom(value)).build();
			ClientMessage cm = ClientMessage.newBuilder().setPutMessage(pm).build();
			int partition = findPartition(key);
			String serverId = dcId + "_" + partition;
			if (sendToServer(serverId, cm) == NetworkStatus.FAILURE)
				return false;
			ClientReply cr = readFromServer(serverId);
			if (cr != null && cr.getStatus()) {
				return true;
			} else {
				protocolLOGGER.severe("Server could not put the key= " + key);
				return false;
			}
		} catch (Exception e) {
			protocolLOGGER.severe(Utils.exceptionLogMessge("Failed to put due to exception", e));
			return false;
		}
	}
	
	public byte[] get(String key) {
		try {
			GetMessage gm = GetMessage.newBuilder().setKey(key).build();
			ClientMessage cm = ClientMessage.newBuilder().setGetMessage(gm).build();
			int partition = findPartition(key);
			String serverId = dcId + "_" + partition;
			if (sendToServer(serverId, cm) == NetworkStatus.FAILURE)
				return null;
			ClientReply cr = readFromServer(serverId);
			if (cr != null && cr.getStatus()) {
				return cr.getGetReply().getValue().toByteArray();
			} else {
				protocolLOGGER.severe("Server could not get the key= " + key);
				return null;
			}
		} catch (Exception e) {
			protocolLOGGER.severe(Utils.exceptionLogMessge("Failed to get due to exception", e));
			return null;
		}
	}

	private int findPartition(String key) throws NoSuchAlgorithmException {
		long hash = Utils.getMd5HashLong(key);
		return (int) (hash % numOfPartitions);
	}
}
