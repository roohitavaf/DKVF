package edu.msu.cse.cops.server;

import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.msu.cse.dkvf.ClientMessageAgent;
import edu.msu.cse.dkvf.DKVFServer;
import edu.msu.cse.cops.server.Utils;
import edu.msu.cse.dkvf.Storage.StorageStatus;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;
import edu.msu.cse.dkvf.metadata.Metadata.Dependency;
import edu.msu.cse.dkvf.metadata.Metadata.DependencyCheckMessage;
import edu.msu.cse.dkvf.metadata.Metadata.DependencyResponseMessage;
import edu.msu.cse.dkvf.metadata.Metadata.GetMessage;
import edu.msu.cse.dkvf.metadata.Metadata.GetReply;
import edu.msu.cse.dkvf.metadata.Metadata.PutMessage;
import edu.msu.cse.dkvf.metadata.Metadata.PutReply;
import edu.msu.cse.dkvf.metadata.Metadata.Record;
import edu.msu.cse.dkvf.metadata.Metadata.ReplicateMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;

public class COPSServer extends DKVFServer {

	class RecordDependecies {
		Record record;
		List<Dependency> deps;

		public RecordDependecies(Record rec, List<Dependency> deps) {
			this.record = rec;
			this.deps = deps;
		}
	}

	int dcId;// datacenter id
	int pId; // partition id
	int numOfDatacenters;
	int numOfPartitions;

	Long clock = new Long(0); // higher bits are Lamport clocks

	// dependency check mechanism
	HashMap<String, List<DependencyCheckMessage>> waitingDepChecks; //the key is the key that we want as dependency, and value is the dependency that is waiting for this key.
	HashMap<String, List<String>> waitingLocalDeps; //the key is the key that we want as dependency, the value is the list of pending keys that are waiting. Both dependency key and pending key are hosted on this partition, that why it is called local dep check. 
	HashMap<String, RecordDependecies> pendingKeys; //the key is the key that is pending, and the value is the record to write + its dependencies. 

	public COPSServer(ConfigReader cnfReader) {
		super(cnfReader);

		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();

		dcId = new Integer(protocolProperties.get("dc_id").get(0));
		pId = new Integer(protocolProperties.get("p_id").get(0));

		numOfDatacenters = new Integer(protocolProperties.get("num_of_datacenters").get(0));
		numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));

		waitingDepChecks = new HashMap<>();
		waitingLocalDeps = new HashMap<>();
		pendingKeys = new HashMap<>();

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
		List<Record> result = new ArrayList<>();
		StorageStatus ss = read(gm.getKey(), (Record rec) -> {
			return true;
		}, result);
		ClientReply cr = null;
		if (ss == StorageStatus.SUCCESS) {
			Record rec = result.get(0);
			cr = ClientReply.newBuilder().setStatus(true).setGetReply(GetReply.newBuilder().setRecord(rec)).build();
		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}
		cma.sendReply(cr);
	}

	private void handlePutMessage(ClientMessageAgent cma) {
		PutMessage pm = cma.getClientMessage().getPutMessage();
		long veriosn = getNextVersion();
		Record rec = Record.newBuilder().setValue(pm.getValue()).setVersion(veriosn).build();
		boolean result = makeVisible(pm.getKey(), rec);
		ClientReply cr = null;
		if (result) {
			cr = ClientReply.newBuilder().setStatus(true).setPutReply(PutReply.newBuilder().setVersion(veriosn)).build();
		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}
		sendReplicateMessages(pm.getKey(), rec, pm.getNearestList());
		cma.sendReply(cr);

	}

	private void sendReplicateMessages(String key, Record recordToReplicate, List<Dependency> nearest) {
		ServerMessage sm = ServerMessage.newBuilder().setReplicateMessage(ReplicateMessage.newBuilder().setKey(key).setRec(recordToReplicate).addAllNearest(nearest)).build();
		for (int i = 0; i < numOfDatacenters; i++) {
			if (i == dcId)
				continue;
			String id = i + "_" + pId;

			protocolLOGGER.finer(MessageFormat.format("Sendng replicate message to {0}: {1}", id, sm.toString()));
			sendToServerViaChannel(id, sm);
		}
	}

	public void handleServerMessage(ServerMessage sm) {
		if (sm.hasReplicateMessage()) {
			handleReplicateMessage(sm);
		} else if (sm.hasDepCheckMessage()) {
			handleDepCheckMessage(sm);
		} else if (sm.hasDepResponseMessage()) {
			handleDepResponseMessage(sm);
		}
	}

	private void handleDepResponseMessage(ServerMessage sm) {
		DependencyResponseMessage drm = sm.getDepResponseMessage();
		int metIndex = -1;
		if (pendingKeys.containsKey(drm.getForKey())) {
			for (int i = 0; i < pendingKeys.get(drm.getForKey()).deps.size(); i++) {
				Dependency dep = pendingKeys.get(drm.getForKey()).deps.get(i);
				if (dep.getKey().equals(drm.getDep().getKey())) {
					if (dep.getVersion() <= drm.getDep().getVersion()) {
						metIndex = i;
						break;
					}
				}
			}
		}
		if (metIndex >= 0) {
			pendingKeys.get(drm.getForKey()).deps.remove(metIndex);
			if (pendingKeys.get(drm.getForKey()).deps.isEmpty()) {
				makeVisible(drm.getForKey(), pendingKeys.get(drm.getForKey()).record);
				//pendingKeys.remove(drm.getForKey());
			}
		}

	}

	private boolean makeVisible(String key, Record rec) {
		// first we check the current verison, maybe it is higher than the
		// version that we want to write. In that case we don't write it.
		List<Record> result = new ArrayList<>();
		StorageStatus ss = read(key, (Record rec2) -> {
			return true;
		}, result);
		if (ss == StorageStatus.SUCCESS) {
			Record currentRec = result.get(0);
			if (currentRec.getVersion() >= rec.getVersion())
				return true;
		}
		// If current version is older, we go ahead, and write the given
		// version.
		ss = insert(key, rec);
		if (ss == StorageStatus.SUCCESS) {
			postVisibility(key, rec.getVersion());
			return true;
		} else
			return false;
	}

	private void handleDepCheckMessage(ServerMessage sm) {
		DependencyCheckMessage cdm = sm.getDepCheckMessage();

		List<Record> result = new ArrayList<>();
		StorageStatus ss = read(cdm.getDep().getKey(), (Record rec) -> {
			return true;
		}, result);
		if (ss == StorageStatus.SUCCESS) {
			Record rec = result.get(0);
			if (rec.getVersion() >= cdm.getDep().getVersion()) {
				sendDepResponse(cdm.getPId(), cdm.getForKey(), cdm.getDep());
				return;
			}
		}
		addToWaitingDepChecks(cdm);
	}

	private void addToWaitingDepChecks(DependencyCheckMessage cdm) {
		if (waitingDepChecks.containsKey(cdm.getDep().getKey())) {
			waitingDepChecks.get(cdm.getDep().getKey()).add(cdm);
		} else {
			List<DependencyCheckMessage> cdms = new ArrayList<>();
			cdms.add(cdm);
			waitingDepChecks.put(cdm.getDep().getKey(), cdms);
		}
	}

	private void handleReplicateMessage(ServerMessage sm) {
		protocolLOGGER.finer(MessageFormat.format("Received replicate message: {0}", sm.toString()));
		ReplicateMessage rm = sm.getReplicateMessage();
		updateClock(rm.getRec().getVersion());

		synchronized (pendingKeys) {
			List<Dependency> remainingDeps = localDepChecking(rm.getKey(), rm.getNearestList());
			if (remainingDeps.isEmpty()) {
				makeVisible(rm.getKey(), rm.getRec());
			} else {
				List<Dependency> deps = new ArrayList<>();
				for (Dependency dep : rm.getNearestList()) {
					deps.add(dep);
				}
				pendingKeys.put(rm.getKey(), new RecordDependecies(rm.getRec(), deps));
				sendDepCheckMessages(rm.getKey(), remainingDeps);
			}
		}
	}

	private void sendDepCheckMessages(String key, List<Dependency> nearestList) {
		for (Dependency dep : nearestList) {
			DependencyCheckMessage dcm = DependencyCheckMessage.newBuilder().setForKey(key).setDep(dep).setPId(pId).build();
			ServerMessage sm = ServerMessage.newBuilder().setDepCheckMessage(dcm).build();
			int partition;
			try {
				partition = findPartition(key);
			} catch (NoSuchAlgorithmException e) {
				protocolLOGGER.severe("Problem finding partition for key " + key);
				return;
			}
			if (partition != pId) {
				String serverId = dcId + "_" + partition;
				sendToServerViaChannel(serverId, sm);
			}
		}

	}

	/**
	 * 
	 * @param key
	 *            Pending key, the key for which we are doing dependency
	 *            checking.
	 * @param deps
	 *            List of dependencies
	 * @return List of remaining dependencies (local/non-local)
	 */
	private List<Dependency> localDepChecking(String key, List<Dependency> deps) {
		List<Dependency> remaining = new ArrayList<>();
		try {
			if (deps != null) {
				for (Dependency dep : deps) {
					int hostingPartition = 0;
					try {
						hostingPartition = findPartition(dep.getKey());
					} catch (NoSuchAlgorithmException e) {
						protocolLOGGER.severe("Problem finding hosting partition for key " + dep.getKey());
						continue;
					}
					if (hostingPartition != pId) {
						remaining.add(dep);
					} else {
						List<Record> result = new ArrayList<>();
						StorageStatus ss = read(dep.getKey(), (Record rec) -> {
							return true;
						}, result);
						if (ss != StorageStatus.SUCCESS || result.get(0).getVersion() < dep.getVersion()) {
							remaining.add(dep);
							addToWaitingLocalDeps(dep.getKey(), key);
						}
					}
				}
			}
		} catch (Exception e) {
			protocolLOGGER.severe(edu.msu.cse.dkvf.Utils.exceptionLogMessge("Error in local dep checking", e));
		}
		return remaining;
	}

	private void addToWaitingLocalDeps(String wantedKey, String pendingKey) {
		if (waitingLocalDeps.containsKey(wantedKey)) {
			if (!waitingLocalDeps.get(wantedKey).contains(pendingKey)) {
				waitingLocalDeps.get(wantedKey).add(pendingKey);
			}
		} else {
			List<String> newPendingKeyList = new ArrayList<String>();
			newPendingKeyList.add(pendingKey);
			waitingLocalDeps.put(wantedKey, newPendingKeyList);
		}
	}

	private int findPartition(String key) throws NoSuchAlgorithmException {
		long hash = edu.msu.cse.dkvf.Utils.getMd5HashLong(key);
		return (int) (hash % numOfPartitions);
	}

	private long getNextVersion() {
		updateClock();
		long lowerBits = Utils.getLowerBits(dcId);
		return clock + lowerBits;
	}

	private void updateClock(long newVersion) {
		synchronized (clock) {
			//Lamport logical clock algorithm
			long newVersionInHigherBits = Utils.getHigherBits(newVersion);

			long newHigherBits = Math.max(clock, newVersionInHigherBits) + Utils.shiftToHighBits(1);
			clock = newHigherBits;
		}

	}

	private void updateClock() {
		synchronized (clock) {
			clock = clock + Utils.shiftToHighBits(1);
		}

	}

	private void postVisibility(String key, long version) {
		//Two types of partitions may wait for visibility of version: 1) local partition, or 2) antoher parition. 
		//We check both cases. 

		//local dep check 
		try {
			synchronized (pendingKeys) {
				if (waitingLocalDeps.containsKey(key)) {
					for (String pendingKey : waitingLocalDeps.get(key)) {
						List<Dependency> oldPending = new ArrayList<>();
						if (pendingKeys.containsKey(pendingKey)) {
							for (Dependency dep : pendingKeys.get(pendingKey).deps)
								oldPending.add(dep);
							for (Dependency dep : oldPending) {
								if (dep.getKey().equals(key) && dep.getVersion() <= version) {
									pendingKeys.get(pendingKey).deps.remove(dep);
									if (pendingKeys.get(pendingKey).deps.isEmpty()) {
										makeVisible(pendingKey, pendingKeys.get(pendingKey).record);
										//pendingKeys.remove(pendingKey);
									}
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			protocolLOGGER.severe(edu.msu.cse.dkvf.Utils.exceptionLogMessge("Error in PostVisibility: ", e));

		}

		//check othre requests from other partitions		
		if (waitingDepChecks.containsKey(key)) {
			for (DependencyCheckMessage dcm : waitingDepChecks.get(key)) {
				if (dcm.getDep().getVersion() <= version) {
					sendDepResponse(dcm.getPId(), dcm.getForKey(), dcm.getDep());
				}
			}
		}

	}

	private void sendDepResponse(int requesterPId, String forKey, Dependency dep) {
		DependencyResponseMessage drm = DependencyResponseMessage.newBuilder().setForKey(forKey).setDep(dep).build();
		ServerMessage sm = ServerMessage.newBuilder().setDepResponseMessage(drm).build();
		String id = dcId + "_" + requesterPId;
		sendToServerViaChannel(id, sm);
	}
}
