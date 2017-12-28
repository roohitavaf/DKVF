package edu.msu.cse.gentleRain.client;

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


public class GentleRainClient extends DKVFClient {

	Long gst = new Long(0);
	Long dt = new Long(0);
	int dcId; 
	
	int numOfPartitions;
	
	//This is just for test to enforce round-robin writes
	int currentPartition = 0;

	public GentleRainClient(ConfigReader cnfReader) {
		super(cnfReader);
		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();
		numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));
		dcId = new Integer(protocolProperties.get("dc_id").get(0));
		
	}

	@Override
	public boolean put(String key, byte[] value) {
		try {
			ClientMessage cm = ClientMessage.newBuilder().setPutMessage(PutMessage.newBuilder().setDt(dt).setKey(key).setValue(ByteString.copyFrom(value))).build();
			
			int partition = findPartition(key);
			String serverId = dcId + "_" + partition;
			if (sendToServer(serverId, cm) == NetworkStatus.FAILURE)
				return false;
			ClientReply cr = readFromServer(serverId);
			if (cr != null && cr.getStatus()) {
				dt = Math.max(dt, cr.getPutReply().getUt());
				return true;
			} else {
				protocolLOGGER.severe("Server could not put the key= " + key);
				return false;
			}
		} catch (Exception e) {
			protocolLOGGER.severe (Utils.exceptionLogMessge("Failed to put due to exception", e));
			return false;
		}
	}

	@Override
	public byte[] get(String key) {
		try {
			GetMessage gm = GetMessage.newBuilder().setGst(gst).setKey(key).build();
			ClientMessage cm = ClientMessage.newBuilder().setGetMessage(gm).build();
			String partition = findPartition(key) + "";
			String serverId = dcId + "_" + partition;
			if (sendToServer(serverId, cm) == NetworkStatus.FAILURE)
				return null;
			ClientReply cr = readFromServer(serverId);
			if (cr != null && cr.getStatus()){
				dt = Math.max(dt, cr.getPutReply().getUt());
				gst = Math.max(gst, cr.getGetReply().getGst());
				return cr.getGetReply().getValue().toByteArray();
			} else {
				protocolLOGGER.severe("Server could not get the key= " + key);
				return null;
			}
		} catch (Exception e) {
			protocolLOGGER.severe (Utils.exceptionLogMessge("Failed to get due to exception", e));
			return null;
		}
	}

	private int findPartition(String key) throws NoSuchAlgorithmException  {
		long hash = Utils.getMd5HashLong(key);
		return (int) (hash % numOfPartitions);
	}

}
