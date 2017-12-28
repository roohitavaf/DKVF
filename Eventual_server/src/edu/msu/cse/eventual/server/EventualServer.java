package edu.msu.cse.eventual.server;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.msu.cse.dkvf.ClientMessageAgent;
import edu.msu.cse.dkvf.DKVFServer;
import edu.msu.cse.dkvf.Storage.StorageStatus;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;
import edu.msu.cse.dkvf.metadata.Metadata.GetMessage;
import edu.msu.cse.dkvf.metadata.Metadata.GetReply;
import edu.msu.cse.dkvf.metadata.Metadata.PutReply;
import edu.msu.cse.dkvf.metadata.Metadata.Record;
import edu.msu.cse.dkvf.metadata.Metadata.ReplicateMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;

public class EventualServer extends DKVFServer {
	ConfigReader cnfReader;

	int numOfDatacenters;
	int dcId;
	int pId;

	public EventualServer(ConfigReader cnfReader) {
		super(cnfReader);
		this.cnfReader = cnfReader;
		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();
		numOfDatacenters = new Integer(protocolProperties.get("num_of_datacenters").get(0));
		dcId = new Integer(protocolProperties.get("dc_id").get(0));
		pId = new Integer(protocolProperties.get("p_id").get(0));
	}

	public void handleClientMessage(ClientMessageAgent cma) {
		if (cma.getClientMessage().hasGetMessage()) {
			handleGetMessage(cma);
		} else if (cma.getClientMessage().hasPutMessage()) {
			handlePutMessage(cma);
		}
	}

	private void handlePutMessage(ClientMessageAgent cma) {
		Record.Builder builder = Record.newBuilder();
		builder.setValue(cma.getClientMessage().getPutMessage().getValue());
		builder.setUt(System.currentTimeMillis());
		Record rec = builder.build();
		StorageStatus ss = insert(cma.getClientMessage().getPutMessage().getKey(), rec);

		ClientReply cr = null;

		if (ss == StorageStatus.SUCCESS) {
			cr = ClientReply.newBuilder().setStatus(true).setPutReply(PutReply.newBuilder().setUt(rec.getUt())).build();
		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}

		cma.sendReply(cr);
		sendReplicateMessages(cma.getClientMessage().getPutMessage().getKey(), rec);
	}

	private void sendReplicateMessages(String key, Record recordToReplicate) {
		ServerMessage sm = ServerMessage.newBuilder().setReplicateMessage(ReplicateMessage.newBuilder().setKey(key).setRec(recordToReplicate)).build();
		for (int i = 0; i < numOfDatacenters; i++) {
			if (i == dcId)
				continue;
			String id = i + "_" + pId;

			protocolLOGGER.finer(MessageFormat.format("Sendng replicate message to {0}: {1}", id, sm.toString()));
			sendToServerViaChannel(id, sm);
		}
	}

	private void handleGetMessage(ClientMessageAgent cma) {
		GetMessage gm = cma.getClientMessage().getGetMessage();
		List<Record> result = new ArrayList<>();
		StorageStatus ss = read(gm.getKey(), p -> {
			return true;
		}, result);
		ClientReply cr = null;
		if (ss == StorageStatus.SUCCESS) {
			Record rec = result.get(0);
			cr = ClientReply.newBuilder().setStatus(true).setGetReply(GetReply.newBuilder().setValue(rec.getValue())).build();
		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}
		cma.sendReply(cr);
	}

	public void handleServerMessage(ServerMessage sm) {
		Record newRecord = sm.getReplicateMessage().getRec();
		insert(sm.getReplicateMessage().getKey(), newRecord);
	}
}
