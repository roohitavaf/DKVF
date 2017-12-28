package edu.msu.cse.causalSpartan.server;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import edu.msu.cse.dkvf.ClientMessageAgent;
import edu.msu.cse.dkvf.DKVFServer;
import edu.msu.cse.dkvf.Storage.StorageStatus;
import edu.msu.cse.causalSpartan.server.Utils;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;
import edu.msu.cse.dkvf.metadata.Metadata.DSVMessage;
import edu.msu.cse.dkvf.metadata.Metadata.DcTimeItem;
import edu.msu.cse.dkvf.metadata.Metadata.GetMessage;
import edu.msu.cse.dkvf.metadata.Metadata.GetReply;
import edu.msu.cse.dkvf.metadata.Metadata.PutMessage;
import edu.msu.cse.dkvf.metadata.Metadata.PutReply;
import edu.msu.cse.dkvf.metadata.Metadata.Record;
import edu.msu.cse.dkvf.metadata.Metadata.ReplicateMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;

public class CausalSpartanServer extends DKVFServer {

	List<Long> dsv;
	int dcId;// datacenter id
	int pId; // partition id
	int numOfDatacenters;
	int numOfPartitions;

	// GST computation
	ArrayList<AtomicLong> vv;
	HashMap<Integer, List<Long>> childrenVvs;

	// Tree structure
	List<Integer> childrenPIds;
	int parentPId;

	// intervals
	int heartbeatInterval;
	int dsvComutationInterval;

	// Heartbeat
	long timeOfLastRepOrHeartbeat;

	Object putLock = new Object(); // It is necessary to make sure that
									// replicates are send
	// FIFO

	public CausalSpartanServer(ConfigReader cnfReader) {
		super(cnfReader);
		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();

		dcId = new Integer(protocolProperties.get("dc_id").get(0));
		pId = new Integer(protocolProperties.get("p_id").get(0));

		parentPId = new Integer(protocolProperties.get("parent_p_id").get(0));
		childrenPIds = new ArrayList<Integer>();
		if (protocolProperties.get("children_p_ids") != null) {
			for (String id : protocolProperties.get("children_p_ids")) {
				childrenPIds.add(new Integer(id));
			}
		}

		numOfDatacenters = new Integer(protocolProperties.get("num_of_datacenters").get(0));
		numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));

		heartbeatInterval = new Integer(protocolProperties.get("heartbeat_interval").get(0));
		dsvComutationInterval = new Integer(protocolProperties.get("dsv_comutation_interval").get(0));

		vv = new ArrayList<>();
		ArrayList<Long> allZero = new ArrayList<>();
		for (int i = 0; i < numOfDatacenters; i++) {
			vv.add(i, new AtomicLong(0));
			allZero.add(new Long(0));
		}
		
		
		childrenVvs = new HashMap<>();
		for (int cpId: childrenPIds){
			childrenVvs.put(cpId, allZero);
		}

		dsv = new ArrayList<>();
		for (int i = 0; i < numOfDatacenters; i++) {
			dsv.add(i, new Long(0));
		}

		// Scheduling periodic operations
		ScheduledExecutorService heartbeatTimer = Executors.newScheduledThreadPool(1);
		ScheduledExecutorService dsvComputationTimer = Executors.newScheduledThreadPool(1);

		heartbeatTimer.scheduleAtFixedRate(new HeartbeatSender(this), 0, heartbeatInterval, TimeUnit.MILLISECONDS);
		dsvComputationTimer.scheduleAtFixedRate(new DsvComputation(this), 0, dsvComutationInterval, TimeUnit.MILLISECONDS);
	}

	public void handleClientMessage(ClientMessageAgent cma) {
		if (cma.getClientMessage().hasGetMessage()) {
			handleGetMessage(cma);
		} else if (cma.getClientMessage().hasPutMessage()) {
			handlePutMessage(cma);
		}

	}

	private void handleGetMessage(ClientMessageAgent cma) {
		GetMessage gm = cma.getClientMessage().getGetMessage();
		updateDsv(gm.getDsvItemList());
		List<Record> result = new ArrayList<>();
		StorageStatus ss = read(gm.getKey(), isVisible, result);
		ClientReply cr = null;
		if (ss == StorageStatus.SUCCESS) {
			Record rec = result.get(0);
			List<DcTimeItem> newDs = updateDS(rec.getSr(), rec.getUt(),rec.getDsItemList());
			cr = ClientReply.newBuilder().setStatus(true).setGetReply(GetReply.newBuilder().setValue(rec.getValue()).addAllDsItem(newDs).addAllDsvItem(dsv)).build();
		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}
		cma.sendReply(cr);
	}

	private void updateDsv(List<Long> dsvItemList) {
		if (dsvItemList == null || dsvItemList.isEmpty())
			return;
		synchronized (dsv) {
			for (int i = 0; i < dsv.size(); i++) {
				dsv.set(i, Math.max(dsv.get(i), dsvItemList.get(i)));
			}
		}

	}

	Predicate<Record> isVisible = (Record r) -> {
		if (dcId == r.getSr())
			return true;

		for (int i = 0; i < r.getDsItemCount(); i++) {
			DcTimeItem dti = r.getDsItem(i);
			if (dsv.get(dti.getDcId()) < dti.getTime())
				return false;
		}
		return true;

	};

	private void handlePutMessage(ClientMessageAgent cma) {
		PutMessage pm = cma.getClientMessage().getPutMessage();
		long dt = Utils.maxDsTime(pm.getDsItemList());
		updateHlc(dt);
		Record rec = null;
		synchronized (putLock) {
			rec = Record.newBuilder().setValue(pm.getValue()).setUt(vv.get(dcId).get()).setSr(dcId).addAllDsItem(pm.getDsItemList()).build();
			sendReplicateMessages(pm.getKey(),rec); // The order is different than the paper
										// algorithm. We first send replicate to
										// insure a version with smaller
										// timestamp is replicated sooner.
		}
		StorageStatus ss = insert(pm.getKey(), rec);
		ClientReply cr = null;

		if (ss == StorageStatus.SUCCESS) {
			cr = ClientReply.newBuilder().setStatus(true).setPutReply(PutReply.newBuilder().setUt(rec.getUt()).setSr(dcId)).build();
		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}
		cma.sendReply(cr);
	}

	private void sendReplicateMessages(String key, Record recordToReplicate) {
		ServerMessage sm = ServerMessage.newBuilder().setReplicateMessage(ReplicateMessage.newBuilder().setDcId(dcId).setKey(key).setRec(recordToReplicate)).build();
		for (int i = 0; i < numOfDatacenters; i++) {
			if (i == dcId)
				continue;
			String id = i + "_" + pId;

			protocolLOGGER.finer(MessageFormat.format("Sendng replicate message to {0}: {1}", id, sm.toString()));
			sendToServerViaChannel(id, sm);
		}
		timeOfLastRepOrHeartbeat = Utils.getPhysicalTime(); //we don't need to synchronize for it, because it is not critical
	}

	private void updateHlc(long dt) {
		long vv_l = Utils.getL(vv.get(dcId).get());
		long physicalTime = Utils.getPhysicalTime();
		long dt_l = Utils.getL(dt);

		long newL = Math.max(Math.max(vv_l, dt_l), Utils.shiftToHighBits(physicalTime));

		long vv_c = Utils.getC(vv.get(dcId).get());
		long dt_c = Utils.getC(dt);
		long newC;
		if (newL == vv_l && newL == dt_l)
			newC = Math.max(vv_c, dt_c) + 1;
		else if (newL == vv_l)
			newC = vv_c + 1;
		else if (newL == dt_l)
			newC = dt_c + 1;
		else
			newC = 0;
		vv.get(dcId).set(newL + newC);
	}

	void updateHlc() {
		long vv_l = Utils.getL(vv.get(dcId).get());
		long physicalTime = Utils.getPhysicalTime();

		long newL = Math.max(vv_l, Utils.shiftToHighBits(physicalTime));

		long vv_c = Utils.getC(vv.get(dcId).get());
		long newC;
		if (newL == vv_l)
			newC = vv_c + 1;
		else
			newC = 0;
		vv.get(dcId).set(newL + newC);
	}

	public void handleServerMessage(ServerMessage sm) {
		if (sm.hasReplicateMessage()) {
			handleReplicateMessage(sm);
		} else if (sm.hasHeartbeatMessage()) {
			handleHearbeatMessage(sm);
		} else if (sm.hasVvMessage()) {
			handleVvMessage(sm);
		} else if (sm.hasDsvMessage()) {
			handleDsvMessage(sm);
		}
	}

	private void handleReplicateMessage(ServerMessage sm) {
		protocolLOGGER.finer(MessageFormat.format("Received replicate message: {0}", sm.toString()));
		int senderDcId = sm.getReplicateMessage().getDcId();
		Record d = sm.getReplicateMessage().getRec();
		insert(sm.getReplicateMessage().getKey(), d);
		vv.get(senderDcId).set(d.getUt());
	}

	void handleHearbeatMessage(ServerMessage sm) {
		int senderDcId = sm.getHeartbeatMessage().getDcId();
		vv.get(senderDcId).set(sm.getHeartbeatMessage().getTime());
	}

	void handleVvMessage(ServerMessage sm) {
		int senderPId = sm.getVvMessage().getPId();
		List<Long> receivedVv = sm.getVvMessage().getVvItemList();
		protocolLOGGER.finest("Recieved" + sm.toString());
		childrenVvs.put(senderPId, receivedVv);
	}

	void handleDsvMessage(ServerMessage sm) {
		protocolLOGGER.finest(sm.toString());
		setDsv(sm.getDsvMessage().getDsvItemList());
		sm = ServerMessage.newBuilder().setDsvMessage(DSVMessage.newBuilder().addAllDsvItem(dsv)).build();
		sendToAllChildren(sm);
	}

	void sendToAllChildren(ServerMessage sm) {
		for (Map.Entry<Integer, List<Long>> child : childrenVvs.entrySet()) {
			int childId = child.getKey();
			sendToServerViaChannel(dcId + "_" + childId, sm);
		}
	}

	void setDsv(List<Long> newDsv) {
		synchronized (dsv) {
			for (int i=0; i<newDsv.size();i++)
				dsv.set(i, newDsv.get(i));	
		}
	}
	
	private List<DcTimeItem> updateDS(int dc, long time, List<DcTimeItem> ds) {
		List<DcTimeItem> result = new ArrayList<>();
		
		for (int i=0; i < ds.size(); i++){
			if (ds.get(i).getDcId() != dc)
				result.add(ds.get(i));
			else {
				long oldTime = ds.get(i).getTime();
				result.add(DcTimeItem.newBuilder().setDcId(dc).setTime(Math.max(time, oldTime)).build());
			}
		}
		return result;
	}
}
