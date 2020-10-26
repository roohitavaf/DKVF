package edu.msu.cse.dkvf.clusterDesigner;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.Rectangle;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.border.LineBorder;

public class Component extends JLabel implements Serializable {

	volatile int screenX = 0;
	volatile int screenY = 0;
	volatile int myX = 0;
	volatile int myY = 0;
	String id;
	String imageAddress;
	public JFrame parameters;

	public Component(Component that) {
		this(that.imageAddress, that.id + "_copy");
	}

	public Component(String imageAddress, String id) {
		this.id = id;
		this.imageAddress = imageAddress;

		//setBorder(new LineBorder(Color.BLUE, 3));
		//setBackground(Color.WHITE);
		// setBounds(0, 0, 200, 200);
		/*
		Image image = null;
		try {
			image = ImageIO.read(new File(imageAddress));
		} catch (Exception e) {
			// TODO: handle exception
		}*/
		URL url = Component.class.getResource(imageAddress);
		ImageIcon icon = new ImageIcon(url);

		//Icon icon = new ImageIcon(image);
		setBounds(0, 0, icon.getIconWidth() + 40, icon.getIconHeight() + 30);
		setText(id);
		setIcon(icon);
		setIconTextGap(0);
		setVerticalTextPosition(JLabel.BOTTOM);
		setHorizontalTextPosition(JLabel.CENTER);
		setOpaque(false);

		addMouseListener(new ComponentMouseListener(this));
		addMouseMotionListener(new MouseMotionListener() {

			@Override
			public void mouseDragged(MouseEvent e) {
				int deltaX = e.getXOnScreen() - screenX;
				int deltaY = e.getYOnScreen() - screenY;

				setLocation(myX + deltaX, myY + deltaY);
				ClusterDesignerApplication.panel.repaint();
				if (deltaX > 20 || deltaY > 20)
					ClusterDesignerApplication.point1 = null;

			}

			@Override
			public void mouseMoved(MouseEvent e) {
			}

		});
	}

	public void startMotion(int x, int y) {
		screenX = x;
		screenY = y;
		myX = getX();
		myY = getY();
	}

	public int getCenterX() {
		Rectangle rec = getBounds();
		return getX() + (int) (rec.getWidth() / 4);
	}

	public int getCenterY() {
		Rectangle rec = getBounds();
		return getY() + (int) (rec.getHeight() / 2);
	}

	public String getId() {
		return id;
	}

	public void updateId(String id) {
		if (this.getClass().equals(ServerComponent.class)) {
			ClusterDesignerApplication.servers.remove(this.id);
			ClusterDesignerApplication.servers.put(id, (ServerComponent) this);
		} else if (this.getClass().equals(ClientComponent.class)) {
			ClusterDesignerApplication.clients.remove(this.id);
			ClusterDesignerApplication.clients.put(id, (ClientComponent) this);
		}
		this.id = id;
		setText(id);
	}

	/**
	 * Post connection routine.
	 * 
	 * @param compnent
	 */
	public void connected(Component compnent) {

	}

	public void disconnect(Component component) {

	}
}