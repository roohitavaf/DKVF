package edu.msu.cse.gentleRain.server;

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
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;
import edu.msu.cse.dkvf.metadata.Metadata.GSTMessage;
import edu.msu.cse.dkvf.metadata.Metadata.GetMessage;
import edu.msu.cse.dkvf.metadata.Metadata.GetReply;
import edu.msu.cse.dkvf.metadata.Metadata.PutMessage;
import edu.msu.cse.dkvf.metadata.Metadata.PutReply;
import edu.msu.cse.dkvf.metadata.Metadata.Record;
import edu.msu.cse.dkvf.metadata.Metadata.ReplicateMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;

public class GentleRainServer extends DKVFServer {
	AtomicLong gst = new AtomicLong(0);
	int dcId;// datacenter id
	int pId; // partition id
	int numOfDatacenters;
	int numOfPartitions;

	// GST computation
	ArrayList<AtomicLong> vv;
	HashMap<Integer, List<Long>> childrenVvs;

	// Tree structure
	int parentPId;
	List<Integer> childrenPIds;

	// intervals
	int heartbeatInterval;
	int gstComutationInterval;

	// Heartbeat
	long timeOfLastRepOrHeartbeat;

	Object putLock = new Object(); // It is necessary to make sure that replicates are send
	// FIFO

	public GentleRainServer(ConfigReader cnfReader) {
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
		gstComutationInterval = new Integer(protocolProperties.get("gst_comutation_interval").get(0));

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

		// Scheduling periodic operations
		ScheduledExecutorService heartbeatTimer = Executors.newScheduledThreadPool(1);
		ScheduledExecutorService gstComputationTimer = Executors.newScheduledThreadPool(1);

		heartbeatTimer.scheduleAtFixedRate(new HeartbeatSender(this), 0, heartbeatInterval, TimeUnit.MILLISECONDS);
		gstComputationTimer.scheduleAtFixedRate(new GstComputation(this), 0, gstComutationInterval, TimeUnit.MILLISECONDS);
	}

	@Override
	public void handleClientMessage(ClientMessageAgent cma) {
		if (cma.getClientMessage().hasGetMessage()) {
			handleGetMessage(cma);
		} else if (cma.getClientMessage().hasPutMessage()) {
			handlePutMessage(cma);
			
		}
	}

	@Override
	public void handleServerMessage(ServerMessage sm) {
		if (sm.hasReplicateMessage()) {
			handleReplicateMessage(sm);
		} else if (sm.hasHeartbeatMessage()) {
			handleHearbeatMessage(sm);
		} else if (sm.hasVvMessage()) {
			handleVvMessage(sm);
		} else if (sm.hasGstMessage()) {
			handleGstMessage(sm);
		}

	}

	private void handleGetMessage(ClientMessageAgent cma) {
		GetMessage gm = cma.getClientMessage().getGetMessage();
		updateGst(gm.getGst());
		List<Record> result = new ArrayList<>();
		StorageStatus ss = read(gm.getKey(), isVisible, result);
		ClientReply cr = null;
		if (ss == StorageStatus.SUCCESS) {
			Record rec = result.get(0);
			cr = ClientReply.newBuilder().setStatus(true).setGetReply(GetReply.newBuilder().setValue(rec.getValue()).setUt(rec.getUt()).setGst(gst.get())).build();
		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}
		cma.sendReply(cr);
	}

	Predicate<Record> isVisible = (Record r) -> {
		protocolLOGGER.finer(MessageFormat.format("record ut= {0}, Current GST={1}", r.getUt(), gst.get()));
		if (dcId == r.getSr() || r.getUt() <= gst.get())
			return true;
		return false;
	};

	private void updateGst(long sample) {
		while (true) {
			long curMax = gst.get();
			if (curMax >= sample) {
				break;
			}
			boolean setSuccessful = gst.compareAndSet(curMax, sample);
			if (setSuccessful) {
				break;
			}
		}
	}

	private void handlePutMessage(ClientMessageAgent cma) {
		PutMessage pm = cma.getClientMessage().getPutMessage();
		long sleepTime = pm.getDt() - System.currentTimeMillis();
		try {
			if (sleepTime > 0){
				Thread.sleep(sleepTime);
				protocolLOGGER.info("Sleeping for " + sleepTime);
			}
		} catch (InterruptedException e) {
			protocolLOGGER.severe("Failed to delay write operation.");
		}
		vv.get(dcId).set(System.currentTimeMillis());
		Record rec = null; 
		
		synchronized (putLock) {
			rec = Record.newBuilder().setValue(pm.getValue()).setUt(vv.get(dcId).get()).setSr(dcId).build();
			sendReplicateMessages(pm.getKey(),rec); // The order is different than the paper
										// algorithm. We first send replicate to
										// insure a version with smaller
										// timestamp is replicated sooner.
		}
		
		StorageStatus ss = insert(pm.getKey(), rec);
		ClientReply cr = null;
		if (ss == StorageStatus.SUCCESS) {
			cr = ClientReply.newBuilder().setStatus(true).setPutReply(PutReply.newBuilder().setUt(rec.getUt())).build();
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
		childrenVvs.put(senderPId, receivedVv);
	}

	void handleGstMessage(ServerMessage sm) {
		Long receivedGst = sm.getGstMessage().getGst();
		gst.set(receivedGst);
		sm = ServerMessage.newBuilder().setGstMessage(GSTMessage.newBuilder().setGst(gst.get())).build();
		sendToAllChildren(sm);
	}

	void sendToAllChildren(ServerMessage sm) {
		for (Map.Entry<Integer, List<Long>> child : childrenVvs.entrySet()) {
			int childId = child.getKey();
			sendToServerViaChannel(dcId + "_" + childId, sm);
		}
	}

}
