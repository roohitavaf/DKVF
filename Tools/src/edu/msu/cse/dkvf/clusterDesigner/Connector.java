package edu.msu.cse.dkvf.clusterDesigner;

import java.awt.Graphics;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.Server;

public class Connector implements Serializable {

	public Component point1;
	public Component point2;
	
	public Connector (Component point1, Component point2){
		this.point1 = point1;
		this.point2 = point2;
	}
	
	public void paint (Graphics g){
		g.drawLine(point1.getCenterX(), point1.getCenterY(), point2.getCenterX(), point2.getCenterY());
	}
	
	public void save (ObjectOutputStream out) throws IOException{
		out.writeObject(point1.id);
		out.writeObject(point1.getClass().getName());
		
		out.writeObject(point2.id);
		out.writeObject(point2.getClass().getName());
	}
	
	public static Connector load (ObjectInputStream input) throws ClassNotFoundException, IOException{
		String point1Name = (String) input.readObject();
		String point1ClassName = (String)input.readObject();
		Component point1 = null; 
		if (point1ClassName.equals(ServerComponent.class.getName())){
			point1 = ClusterDesignerApplication.servers.get(point1Name);
		}else if (point1ClassName.equals(ClientComponent.class.getName())){
			point1 = ClusterDesignerApplication.clients.get(point1Name);
		}else if (point1ClassName.equals(HubComponent.class.getName())){
			point1 = ClusterDesignerApplication.hubs.get(point1Name);
		}
		
		String point2Name = (String) input.readObject();
		String point2ClassName = (String)input.readObject();
		Component point2 = null;
		if (point2ClassName.equals(ServerComponent.class.getName())){
			point2 = ClusterDesignerApplication.servers.get(point2Name);
		}else if (point2ClassName.equals(ClientComponent.class.getName())){
			point2 = ClusterDesignerApplication.clients.get(point2Name);
		}else if (point2ClassName.equals(HubComponent.class.getName())){
			point2 = ClusterDesignerApplication.hubs.get(point2Name);
		}
		
		return new Connector(point1, point2);
		
	}
}
