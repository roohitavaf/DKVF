package edu.msu.cse.causalSpartan.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import edu.msu.cse.dkvf.metadata.Metadata.DSVMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;
import edu.msu.cse.dkvf.metadata.Metadata.VVMessage;

public class DsvComputation implements Runnable {

	CausalSpartanServer server;

	public DsvComputation(CausalSpartanServer server) {
		this.server = server;
	}

	@Override
	public void run() {
		//take minimum of all childrens 
		List<Long> minVv = new ArrayList<Long>();
		for (AtomicLong v : server.vv) {
			minVv.add(v.get());
		}
		
		for (Map.Entry<Integer, List<Long>> childVv : server.childrenVvs.entrySet()) {
			for (int i = 0; i < childVv.getValue().size(); i++) {
				if (minVv.get(i) > childVv.getValue().get(i))
					minVv.set(i, childVv.getValue().get(i));
			}
		}

		//if the node is parent it send DsvMessage to its children
		ServerMessage sm = null;
		if (server.parentPId == server.pId) {
			server.setDsv(minVv);
			sm = ServerMessage.newBuilder().setDsvMessage(DSVMessage.newBuilder().addAllDsvItem(minVv)).build();
			server.sendToAllChildren(sm);
		}
		//if the node is not root, it send vvMessage to its parent.
		else {
			VVMessage vvM = VVMessage.newBuilder().setPId(server.pId).addAllVvItem(minVv).build();
			sm = ServerMessage.newBuilder().setVvMessage(vvM).build();
			server.sendToServerViaChannel(server.dcId + "_" + server.parentPId, sm);
		}

	}

}
