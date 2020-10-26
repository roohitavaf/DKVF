package edu.msu.cse.dkvf.clusterDesigner;

import java.awt.Graphics;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.swing.JPanel;

public class ClusterDesignerPanel extends JPanel{
	
	public HashMap<String, Connector> connectors = new HashMap();
	
	@Override
	protected void paintComponent(Graphics g) {
		super.paintComponent(g);
		for (Connector c :connectors.values()){
			c.paint(g);
		}
	}
	
	
	public void clear(){
		connectors.clear();
	}
}
