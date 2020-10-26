package edu.msu.cse.dkvf.clusterDesigner;

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

public class ComponentMouseListener implements MouseListener {

	Component component;
	
	public ComponentMouseListener(Component component) {
		this.component = component;
	}
	
	public void mouseClicked(MouseEvent event)
	{
	  if (event.getClickCount() == 2) {
		  if (component.parameters != null)
			  component.parameters.setVisible(true);
	  }
	}

	@Override
	public void mousePressed(MouseEvent e) {
		component.startMotion(e.getXOnScreen(), e.getYOnScreen());
		
	}

	@Override
	public void mouseReleased(MouseEvent e) {
		if (ClusterDesignerApplication.isConnecting){
			if (ClusterDesignerApplication.point1 == null){
				ClusterDesignerApplication.setPoint1(component);
			}else {
				ClusterDesignerApplication.setPoint2(component);
			}
		}
		
		if (ClusterDesignerApplication.isDisconnecting){
			if (ClusterDesignerApplication.getPoint1() == null){
				ClusterDesignerApplication.setPoint1(component);
			}else {
				ClusterDesignerApplication.setPoint2(component);
			}
		}
		
		ClusterDesignerApplication.selectedComponent= component;
		System.out.println("selected component: " + component.id);
		
	}

	@Override
	public void mouseEntered(MouseEvent e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void mouseExited(MouseEvent e) {
		// TODO Auto-generated method stub
		
	}

}
