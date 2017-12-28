package edu.msu.cse.causalSpartan.client;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

import edu.msu.cse.dkvf.DKVFClient;
import edu.msu.cse.dkvf.ServerConnector.NetworkStatus;
import edu.msu.cse.dkvf.Utils;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.ClientMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;
import edu.msu.cse.dkvf.metadata.Metadata.DcTimeItem;
import edu.msu.cse.dkvf.metadata.Metadata.GetMessage;
import edu.msu.cse.dkvf.metadata.Metadata.PutMessage;

public class CausalSpartanClient extends DKVFClient {

	List<Long> dsv;
	HashMap<Integer, Long> ds;
	int dcId;
	int numOfPartitions;
	int numOfDatacenters;

	public CausalSpartanClient(ConfigReader cnfReader) {
		super(cnfReader);
		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();
		numOfDatacenters = new Integer(protocolProperties.get("num_of_datacenters").get(0));
		numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));

		dcId = new Integer(protocolProperties.get("dc_id").get(0));

		dsv = new ArrayList<>();
		for (int i = 0; i < numOfDatacenters; i++) {
			dsv.add(i, new Long(0));
		}

		ds = new HashMap<>();
	}

	public boolean put(String key, byte[] value) {
		try {
			PutMessage pm = PutMessage.newBuilder().setKey(key).setValue(ByteString.copyFrom(value)).addAllDsItem(getDcTimeItems()).build();
			ClientMessage cm = ClientMessage.newBuilder().setPutMessage(pm).build();
			int partition = findPartition(key);
			String serverId = dcId + "_" + partition;
			if (sendToServer(serverId, cm) == NetworkStatus.FAILURE)
				return false;
			ClientReply cr = readFromServer(serverId);
			if (cr != null && cr.getStatus()) {
				updateDS(dcId, cr.getPutReply().getUt());
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
			GetMessage gm = GetMessage.newBuilder().addAllDsvItem(dsv).setKey(key).build();
			ClientMessage cm = ClientMessage.newBuilder().setGetMessage(gm).build();
			int partition = findPartition(key);
			String serverId = dcId + "_" + partition;
			if (sendToServer(serverId, cm) == NetworkStatus.FAILURE)
				return null;
			ClientReply cr = readFromServer(serverId);
			if (cr != null && cr.getStatus()) {
				updateDsv(cr.getGetReply().getDsvItemList());
				for (DcTimeItem dti : cr.getGetReply().getDsItemList()) {
					updateDS(dti.getDcId(), dti.getTime());
				}
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

	private void updateDS(int dc, long time) {
		if (ds.containsKey(dc))
			ds.put(dc, Math.max(time, ds.get(dc)));
		else {
			ds.put(dc, time);
		}
	}

	private List<DcTimeItem> getDcTimeItems() {
		List<DcTimeItem> result = new ArrayList<>();
		for (Map.Entry<Integer, Long> entry : ds.entrySet()) {
			DcTimeItem dti = DcTimeItem.newBuilder().setDcId(entry.getKey()).setTime(entry.getValue()).build();
			result.add(dti);
		}
		return result;
	}

	private void updateDsv(List<Long> dsvItemList) {
		if (dsvItemList == null || dsvItemList.isEmpty())
			return;
		for (int i = 0; i < dsv.size(); i++) {
			dsv.set(i, Math.max(dsv.get(i), dsvItemList.get(i)));
		}
	}
}
