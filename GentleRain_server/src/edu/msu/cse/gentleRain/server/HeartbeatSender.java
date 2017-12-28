package edu.msu.cse.gentleRain.server;

import edu.msu.cse.dkvf.metadata.Metadata.HeartbeatMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;

public class HeartbeatSender implements Runnable {
	GentleRainServer server;
	public HeartbeatSender(GentleRainServer server) {
		this.server = server;
	}
	@Override
	public void run() {
		long ct = System.currentTimeMillis(); 
		if (ct > server.timeOfLastRepOrHeartbeat + server.heartbeatInterval){
			server.vv.get(server.dcId).set(ct);
			ServerMessage sm = ServerMessage.newBuilder().setHeartbeatMessage(HeartbeatMessage.newBuilder().setDcId(server.dcId).setTime(ct)).build();
			for (int i = 0; i < server.numOfDatacenters; i++) {
				if (i == server.dcId)
					continue;
				server.sendToServerViaChannel(i + "_" + server.pId, sm);
			}
		}

	}

}
