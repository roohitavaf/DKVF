package edu.msu.cse.dkvf.clusterDesigner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ServerInfo;

public class HubComponent extends Component {
	static int numberOfHubComponent = 0;

	Map<String, ServerComponent> servers = new HashMap<>();
	Map<String, ClientComponent> clients = new HashMap();
	Map<String, HubComponent> hubs = new HashMap();

	public HubComponent() {
		super("resources/hub.png", "Hub_" + numberOfHubComponent++);
	}

	public HubComponent(String id) {
		super("resources/hub.png", id);
	}

	public void connected(Component component) {
		if (component.getClass().equals(ServerComponent.class)) {
			servers.put(component.id, (ServerComponent) component);
		} else if (component.getClass().equals(ClientComponent.class)) {
			clients.put(component.id, (ClientComponent) component);
		} else if (component.getClass().equals(HubComponent.class)) {
			hubs.put(component.id, (HubComponent) component);
		}
	}

	public void disconnect(Component component) {
		if (component.getClass().equals(ServerComponent.class)) {
			servers.remove(component.id);
		} else if (component.getClass().equals(ClientComponent.class)) {
			clients.remove(component.id);
		} else if (component.getClass().equals(HubComponent.class)) {
			hubs.remove(component.id);
		}
	}

	public List<String> getServersId(List<String> markedHubs) {
		List<String> results = new ArrayList<>();
		if (markedHubs.contains(id))
			return results;
		markedHubs.add(id);
		for (Map.Entry<String, ServerComponent> s : servers.entrySet()) {
			results.add(s.getKey());
		}
		for (Map.Entry<String, HubComponent> h : hubs.entrySet()) {
			List<String> hServers = h.getValue().getServersId(markedHubs);
			results.addAll(hServers);
		}
		return results;
	}

	public List<ServerInfo> getServersInfo(List<String> markedHubs) {
		List<ServerInfo> results = new ArrayList<>();
		if (markedHubs.contains(id))
			return results;
		markedHubs.add(id);
		for (Map.Entry<String, ServerComponent> s : servers.entrySet()) {
			ServerComponent sc = s.getValue();
			ServerInfo newServer = new ServerInfo();
			newServer.setId(sc.id);
			newServer.setIp(ClusterDesignerApplication.getActualIp(((EditServerParametersFrame) sc.parameters).ipTextField.getText()));
			String port = ClusterDesignerApplication.defaultClusterParameters.clientPortTextField.getText();
			if (((EditServerParametersFrame) sc.parameters).clientPortTextField.getText()!= null &&
					!((EditServerParametersFrame) sc.parameters).clientPortTextField.getText().isEmpty())
				port = ((EditServerParametersFrame) sc.parameters).clientPortTextField.getText();
			newServer.setPort(port);
			results.add(newServer);
		}
		for (Map.Entry<String, HubComponent> h : hubs.entrySet()) {
			List<ServerInfo> hServers = h.getValue().getServersInfo(markedHubs);
			results.addAll(hServers);
		}
		return results;
	}

	public void save(ObjectOutputStream out) throws IOException {
		out.writeObject(id);
		out.writeInt(getX());
		out.writeInt(getY());
		out.writeInt(servers.size());
		for (Map.Entry<String, ServerComponent> sc : servers.entrySet()) {
			out.writeObject(sc.getKey());
		}
		out.writeInt(clients.size());
		for (Map.Entry<String, ClientComponent> cc : clients.entrySet()) {
			out.writeObject(cc.getKey());
		}
		out.writeInt(hubs.size());
		for (Map.Entry<String, HubComponent> hc : hubs.entrySet()) {
			out.writeObject(hc.getKey());
		}
	}

	public static HubComponent load(ObjectInputStream input) throws ClassNotFoundException, IOException {
		String id = (String) input.readObject();
		HubComponent result = new HubComponent(id);
		result.myX = input.readInt();
		result.myY = input.readInt();
		//reading servers
		int nServers = input.readInt();
		
		for (int i = 0; i < nServers; i++) {
			String serverId = (String)input.readObject();
			result.servers.put(serverId, ClusterDesignerApplication.servers.get(serverId));
		}

		//reading clients
		int nClients = input.readInt();
		for (int i = 0; i < nClients; i++) {
			String clientId = (String)input.readObject();
			result.clients.put(clientId, ClusterDesignerApplication.clients.get(clientId));
		}

		//reading hubs
		int nHubs = input.readInt();
		for (int i = 0; i < nHubs; i++) {
			String hubsId = (String)input.readObject();
			result.hubs.put(hubsId, ClusterDesignerApplication.hubs.get(hubsId));
		}

		ClusterDesignerApplication.panel.add(result);
		result.setLocation(result.myX, result.myY);
		return result;
	}

	public void removePoint(Component selectedComponent) {
		if (selectedComponent.getClass().equals(ServerComponent.class)){
			servers.remove(selectedComponent.id);
		}else if (selectedComponent.getClass().equals(ClientComponent.class)){
			clients.remove(selectedComponent.id);
		}else if (selectedComponent.getClass().equals(HubComponent.class)){
			hubs.remove(selectedComponent.id);
		}
		
	}

}