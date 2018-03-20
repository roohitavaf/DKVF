package edu.msu.cse.accf.server;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.sun.corba.se.impl.orb.PropertyOnlyDataCollector;

import edu.msu.cse.dkvf.metadata.Metadata.SVVMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;
import edu.msu.cse.dkvf.metadata.Metadata.VVMessage;

public class SVVComputation implements Runnable {

	ACCFServer server;

	public SVVComputation(ACCFServer server) {
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
				//server.protocolLOGGER.info(MessageFormat.format("###########myVV[{0}]= {1}, childVV[{0}] = {2}", i, minVv.get(i), childVv.getValue().get(i)));
				if (minVv.get(i) > childVv.getValue().get(i)) {
					minVv.set(i, childVv.getValue().get(i));
					//server.protocolLOGGER.info("*Child is smaller*");
				}
			}
		}

		//if the node is parent it send DsvMessage to its children
		ServerMessage sm = null;
		if (server.parentPId == server.pId) {
			server.setSvv(minVv);
			sm = ServerMessage.newBuilder().setSvvMessage(SVVMessage.newBuilder().addAllSvvItem(minVv)).build();
			server.sendToAllChildren(sm);
			//server.protocolLOGGER.info(MessageFormat.format(">>>SVV[0]= {0}", server.svv.get(0)));
		}
		//if the node is not root, it send vvMessage to its parent.
		else {
			VVMessage vvM = VVMessage.newBuilder().setPId(server.pId).addAllVvItem(minVv).build();
			sm = ServerMessage.newBuilder().setVvMessage(vvM).build();
			server.sendToServerViaChannel(server.cg_id + "_" + server.parentPId, sm);
		}

	}

}
