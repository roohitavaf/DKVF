package edu.msu.cse.orion.server;

import edu.msu.cse.dkvf.metadata.Metadata.HeartbeatMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;

public class HeartbeatSender implements Runnable {
    OrionServer server;

    public HeartbeatSender(OrionServer server) {
        this.server = server;
    }

    @Override
    public void run() {
        long ct = System.currentTimeMillis();
        if (ct > server.timeOfLastRepOrHeartbeat + server.heartbeatInterval) {
            server.updateHlc();
            ServerMessage sm = ServerMessage.newBuilder().setHeartbeatMessage(HeartbeatMessage.newBuilder().setDcId(server.dcId).setTime(server.vv.get(server.dcId).get())).build();
            for (int i = 0; i < server.numOfDatacenters; i++) {
                if (i == server.dcId)
                    continue;
                server.sendToServerViaChannel(i + "_" + server.pId, sm);
            }
            server.timeOfLastRepOrHeartbeat = Utils.getPhysicalTime();
        }

    }
}
