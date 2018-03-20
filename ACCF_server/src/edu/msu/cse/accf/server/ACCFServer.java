package edu.msu.cse.accf.server;

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

import edu.msu.cse.accf.server.Utils;
import edu.msu.cse.dkvf.ClientMessageAgent;
import edu.msu.cse.dkvf.DKVFServer;
import edu.msu.cse.dkvf.Storage.StorageStatus;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;
import edu.msu.cse.dkvf.metadata.Metadata.GetMessage;
import edu.msu.cse.dkvf.metadata.Metadata.GetReply;
import edu.msu.cse.dkvf.metadata.Metadata.PutMessage;
import edu.msu.cse.dkvf.metadata.Metadata.PutReply;
import edu.msu.cse.dkvf.metadata.Metadata.Record;
import edu.msu.cse.dkvf.metadata.Metadata.ReplicateMessage;
import edu.msu.cse.dkvf.metadata.Metadata.SVVMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;
import edu.msu.cse.dkvf.metadata.Metadata.TgTimeItem;

public class ACCFServer extends DKVFServer {

	List<Long> svv; //In this implementation, we assume each server is in only one tracking group.
	int cg_id;// checking group id
	int tg_id;// tracking group id
	int pId; // partition id
	int numOfTrackingGroups;
	int numOfPartitions;

	// GST computation
	ArrayList<AtomicLong> vv;
	HashMap<Integer, List<Long>> childrenVvs;

	// Tree structure
	List<Integer> childrenPIds;
	int parentPId;

	// intervals
	int heartbeatInterval;
	int svvComutationInterval;

	
	//simulation parameters
	int messageDelay;  
	
	// Heartbeat
	long timeOfLastRepOrHeartbeat;

	Object putLock = new Object(); // It is necessary to make sure that
									// replicates are send
	// FIFO

	public ACCFServer(ConfigReader cnfReader) {
		super(cnfReader);
		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();

		cg_id = new Integer(protocolProperties.get("cg_id").get(0));
		tg_id = new Integer(protocolProperties.get("tg_id").get(0));
		pId = new Integer(protocolProperties.get("p_id").get(0));

		
		messageDelay = new Integer(protocolProperties.get("message_delay").get(0));
		
		parentPId = new Integer(protocolProperties.get("parent_p_id").get(0));
		childrenPIds = new ArrayList<Integer>();
		if (protocolProperties.get("children_p_ids") != null) {
			for (String id : protocolProperties.get("children_p_ids")) {
				childrenPIds.add(new Integer(id));
			}
		}

		numOfTrackingGroups = new Integer(protocolProperties.get("num_of_tracking_groups").get(0));
		numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));

		heartbeatInterval = new Integer(protocolProperties.get("heartbeat_interval").get(0));
		svvComutationInterval = new Integer(protocolProperties.get("svv_comutation_interval").get(0));

		vv = new ArrayList<>();
		ArrayList<Long> allZero = new ArrayList<>();
		for (int i = 0; i < numOfTrackingGroups; i++) {
			vv.add(i, new AtomicLong(0));
			allZero.add(new Long(0));
		}
		
		
		childrenVvs = new HashMap<>();
		for (int cpId: childrenPIds){
			childrenVvs.put(cpId, allZero);
		}

		svv = new ArrayList<>();
		for (int i = 0; i < numOfTrackingGroups; i++) {
			svv.add(i, new Long(0));
		}

		// Scheduling periodic operations
		ScheduledExecutorService heartbeatTimer = Executors.newScheduledThreadPool(1);
		ScheduledExecutorService dsvComputationTimer = Executors.newScheduledThreadPool(1);

		heartbeatTimer.scheduleAtFixedRate(new HeartbeatSender(this), 0, heartbeatInterval, TimeUnit.MILLISECONDS);
		dsvComputationTimer.scheduleAtFixedRate(new SVVComputation(this), 0, svvComutationInterval, TimeUnit.MILLISECONDS);
		
		protocolLOGGER.info("Server initiated sucessfully");
		channelDelay = messageDelay;
	}

	public void handleClientMessage(ClientMessageAgent cma) {
		if (cma.getClientMessage().hasGetMessage()) {
			handleGetMessage(cma);
		} else if (cma.getClientMessage().hasPutMessage()) {
			handlePutMessage(cma);
		}

	}

	private void handleGetMessage(ClientMessageAgent cma) {
		protocolLOGGER.info(MessageFormat.format("Get message arrived! for key {0}", cma.getClientMessage().getGetMessage().getKey()));
		//In this implementation, we assume the server is in only one checking group. Thus, we don't read client's checking group for now. 
		//wait for SVV
		List<TgTimeItem> ds = cma.getClientMessage().getGetMessage().getDsItemList();
		for (int i = 0; i < ds.size(); i++) {
			TgTimeItem dti = ds.get(i);
			try {
			   // synchronized(svv) {
			        while(vv.get(dti.getTg()).get() < dti.getTime()) {
			        	protocolLOGGER.info(MessageFormat.format("Waiting! vv[{0}] = {1} while ds[{0}]= {2}", dti.getTg(), vv.get(dti.getTg()).get(), dti.getTime()));
			        	//svv.wait();
			        	Thread.sleep(1);
			        }
			   // }
			} catch (InterruptedException e) {
			    protocolLOGGER.severe("Intruption exception while waiting for consistent version");
			}	
		}
		GetMessage gm = cma.getClientMessage().getGetMessage();
		List<Record> result = new ArrayList<>();
		boolean isSvvLargeEnough = true; 
		for (int i = 0; i < ds.size(); i++) {
			TgTimeItem dti = ds.get(i);
			if (svv.get(dti.getTg()) < dti.getTime()) {
				isSvvLargeEnough = false;
				break;
			}
		}
		
		StorageStatus ss; 
		if (isSvvLargeEnough)
			ss = read(gm.getKey(), isVisible, result);
		else 
			ss = read(gm.getKey(), (Record r) -> {return true;}, result);
		ClientReply cr = null;
		if (ss == StorageStatus.SUCCESS) {
			Record rec = result.get(0);
			//protocolLOGGER.info(MessageFormat.format("number of ds items= {0}",  result.get(0).getDsItemCount()));
			//TgTimeItem dti = rec.getDsItem(0);
			//protocolLOGGER.info(MessageFormat.format("svv[{0}]={1}, d.ds[{0}] = {2}", dti.getTg(), svv.get(dti.getTg()), dti.getTime()));
			//protocolLOGGER.info(MessageFormat.format("myVV[{0}]={1}, childVV[{0}] = {2}", dti.getTg(), vv.get(dti.getTg()), childrenVvs.get(1).get(0)));
			
			cr = ClientReply.newBuilder().setStatus(true).setGetReply(GetReply.newBuilder().setD(rec)).build();
		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}
		cma.sendReply(cr);
	}


	Predicate<Record> isVisible = (Record r) -> {
		//protocolLOGGER.info(MessageFormat.format("number of ds items= {0}",  r.getDsItemCount()));
		if (svv.get(r.getTg()) < r.getUt()) {
			return false;
		}
		for (int i = 0; i < r.getDsItemCount(); i++) {
			TgTimeItem dti = r.getDsItem(i);
			//protocolLOGGER.info(MessageFormat.format("ssv[{0}]={1}, d.ds[{0}] = {2}", dti.getTg(), svv.get(dti.getTg()), dti.getTime()));
			if (svv.get(dti.getTg()) < dti.getTime()) {
				protocolLOGGER.info("This version is not consistent, so I don't give to!");
				return false;
				
			}
				
		}
		return true;
	};

	private void handlePutMessage(ClientMessageAgent cma) {
		PutMessage pm = cma.getClientMessage().getPutMessage();
		long dt = Utils.maxDsTime(pm.getDsItemList());
		updateHlc(dt);
		Record rec = null;
		synchronized (putLock) {
			rec = Record.newBuilder().setValue(pm.getValue()).setUt(vv.get(tg_id).get()).setTg(tg_id).addAllDsItem(pm.getDsItemList()).build();
			sendReplicateMessages(pm.getKey(),rec); // The order is different than the paper
										// algorithm. We first send replicate to
										// insure a version with smaller
										// timestamp is replicated sooner.
		}
		StorageStatus ss = insert(pm.getKey(), rec);
		ClientReply cr = null;

		if (ss == StorageStatus.SUCCESS) {
			cr = ClientReply.newBuilder().setStatus(true).setPutReply(PutReply.newBuilder().setUt(rec.getUt()).setTg(tg_id)).build();
		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}
		cma.sendReply(cr);
	}

	private void sendReplicateMessages(String key, Record recordToReplicate) {
		ServerMessage sm = ServerMessage.newBuilder().setReplicateMessage(ReplicateMessage.newBuilder().setTg(tg_id).setKey(key).setD(recordToReplicate)).build();
		for (int i = 0; i < numOfTrackingGroups; i++) { //We can implement different data placement policies here.
			if (i == tg_id)
				continue;
			String id = i + "_" + pId;

			protocolLOGGER.finer(MessageFormat.format("Sendng replicate message to {0}: {1}", id, sm.toString()));
			sendToServerViaChannel(id, sm);
		}
		timeOfLastRepOrHeartbeat = Utils.getPhysicalTime(); //we don't need to synchronize for it, because it is not critical
	}

	private void updateHlc(long dt) {
		long vv_l = Utils.getL(vv.get(tg_id).get());
		long physicalTime = Utils.getPhysicalTime();
		long dt_l = Utils.getL(dt);

		long newL = Math.max(Math.max(vv_l, dt_l), Utils.shiftToHighBits(physicalTime));

		long vv_c = Utils.getC(vv.get(tg_id).get());
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
		vv.get(tg_id).set(newL + newC);
	}

	void updateHlc() {
		long vv_l = Utils.getL(vv.get(tg_id).get());
		long physicalTime = Utils.getPhysicalTime();

		long newL = Math.max(vv_l, Utils.shiftToHighBits(physicalTime));

		long vv_c = Utils.getC(vv.get(tg_id).get());
		long newC;
		if (newL == vv_l)
			newC = vv_c + 1;
		else
			newC = 0;
		vv.get(tg_id).set(newL + newC);
	}

	public void handleServerMessage(ServerMessage sm) {
		if (sm.hasReplicateMessage()) {
			handleReplicateMessage(sm);
		} else if (sm.hasHeartbeatMessage()) {
			handleHearbeatMessage(sm);
		} else if (sm.hasVvMessage()) {
			handleVvMessage(sm);
		} else if (sm.hasSvvMessage()) {
			handleSvvMessage(sm);
		}
	}

	private void handleReplicateMessage(ServerMessage sm) {
		protocolLOGGER.finer(MessageFormat.format("Received replicate message: {0}", sm.toString()));
		int senderTgId = sm.getReplicateMessage().getTg();
		Record d = sm.getReplicateMessage().getD();
		insert(sm.getReplicateMessage().getKey(), d);
		vv.get(senderTgId).set(d.getUt());
	}

	void handleHearbeatMessage(ServerMessage sm) {
		int senderTgId = sm.getHeartbeatMessage().getTg();
		vv.get(senderTgId).set(sm.getHeartbeatMessage().getTime());
	}

	void handleVvMessage(ServerMessage sm) {
		int senderPId = sm.getVvMessage().getPId();
		List<Long> receivedVv = sm.getVvMessage().getVvItemList();
		protocolLOGGER.finest("Recieved" + sm.toString());
		childrenVvs.put(senderPId, receivedVv);
		
	}

	void handleSvvMessage(ServerMessage sm) {
		protocolLOGGER.finest(sm.toString());
		setSvv(sm.getSvvMessage().getSvvItemList());
		sm = ServerMessage.newBuilder().setSvvMessage(SVVMessage.newBuilder().addAllSvvItem(svv)).build();
		sendToAllChildren(sm);
	}

	void sendToAllChildren(ServerMessage sm) {
		for (Map.Entry<Integer, List<Long>> child : childrenVvs.entrySet()) {
			int childId = child.getKey();
			sendToServerViaChannel(cg_id + "_" + childId, sm);
		}
	}

	void setSvv(List<Long> newSvv) {
		synchronized (svv) {
			for (int i=0; i<newSvv.size();i++)
				svv.set(i, newSvv.get(i));	
			svv.notify();
		}
	}
}
