package edu.msu.cse.dkvf.clusterDesigner;

import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.awt.Point;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableModel;
import javax.xml.bind.SchemaOutputResolver;

import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.Property;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Variations;

import javax.swing.JTable;
import javax.swing.JButton;
import javax.swing.JScrollPane;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.awt.event.ActionEvent;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.RowSpec;
import com.jgoodies.forms.layout.FormSpecs;
import java.awt.GridLayout;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import java.awt.CardLayout;
import java.awt.Dimension;
import java.awt.GridBagLayout;
import java.awt.GridBagConstraints;
import net.miginfocom.swing.MigLayout;
import java.awt.FlowLayout;
import static javax.swing.ScrollPaneConstants.*;
import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;

public class EditWorkloadVariations extends JFrame {

	private JPanel contentPane;
	int nVariations = 0;
	JPanel variationPanel;
	
	HashMap<String, JButton> variationButtons = new HashMap<>();
	HashMap<String, EditVariationFrame> variationFrames = new HashMap<>();
	
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					EditWorkloadVariations frame = new EditWorkloadVariations();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * @wbp.parser.constructor
	 */
	public EditWorkloadVariations(EditWorkloadVariations that) {
		this();
		clear();

	}

	/**
	 * Create the frame.
	 */
	public EditWorkloadVariations() {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 196, 339);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);

		JPanel panel = new JPanel();
		contentPane.add(panel, BorderLayout.SOUTH);
		
		JScrollPane scrollPane = new JScrollPane();
		contentPane.add(scrollPane, BorderLayout.CENTER);
		//scrollPane.setHorizontalScrollBarPolicy(HORIZONTAL_SCROLLBAR_NEVER);

		variationPanel = new JPanel();
		scrollPane.setViewportView(variationPanel);
		variationPanel.setSize(variationPanel.getPreferredSize());
		variationPanel.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));

		BoxLayout boxLayout1 = new BoxLayout(variationPanel, BoxLayout.Y_AXIS);
		variationPanel.setLayout(boxLayout1);
		

		JButton btnAddVariation = new JButton("Add");
		btnAddVariation.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String name = "Variration " + nVariations;
				JButton btnNewButton = new JButton(name);
				nVariations++;
				EditVariationFrame evf = new EditVariationFrame(name, EditWorkloadVariations.this);
				evf.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
				variationFrames.put(name, evf);
				variationButtons.put(name, btnNewButton);
				btnNewButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						evf.setVisible(true);
					}
				});
				
				variationPanel.add(btnNewButton);
				validate();
			}
		});
		panel.add(btnAddVariation);
		
		JButton btnNewButton_1 = new JButton("Clear");
		btnNewButton_1.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				clear();
			}
		});
		panel.add(btnNewButton_1);
		
	}

	private void clear() {
		variationPanel.removeAll();
		nVariations = 0;
		variationFrames.clear();
		variationButtons.clear();
		revalidate();
		repaint();
	}
	
	void removeVariation (String variationName){
		variationFrames.remove(variationName);
		JButton variationButton = variationButtons.get(variationName);
		variationPanel.remove(variationButton);
		variationButtons.remove(variationName);
		revalidate();
		repaint();
	}
	
	void updateName (String oldName, String newName){
		EditVariationFrame evf = variationFrames.get(oldName);
		variationFrames.remove(oldName);
		variationFrames.put(newName, evf);
		
		
		JButton variationButton = variationButtons.get(oldName);
		variationButtons.remove(oldName);
		variationButtons.put(newName, variationButton);
		
		variationButton.setText(newName);
	}

	public Variations getWorkloadVariations() {
		Variations variations = new Variations();
		for (EditVariationFrame evf : variationFrames.values()){
			variations.getVariation().add(evf.getVariation());
		}
		return variations;
	}

	public void save(ObjectOutputStream output) throws IOException {
		output.writeInt(nVariations);
		output.writeInt(variationButtons.size());
		for (Map.Entry<String, JButton> button : variationButtons.entrySet()){
			output.writeObject(button.getKey());
		}
		for (Map.Entry<String, EditVariationFrame> button : variationFrames.entrySet()){
			button.getValue().save(output);
		}
	}

	public static EditWorkloadVariations load(ObjectInputStream input) throws IOException, ClassNotFoundException {
		EditWorkloadVariations result = new EditWorkloadVariations();
		result.nVariations = input.readInt();
		int actualSize = input.readInt();
		for (int i=0; i < actualSize; i++){
			String name = (String)input.readObject();
			JButton newButton = new JButton(name);
			result.variationPanel.add(newButton);
			result.variationButtons.put(name, newButton);
		}
		for (int i=0; i < actualSize; i++){
			EditVariationFrame newFrame = EditVariationFrame.load(input, result);
			result.variationFrames.put(newFrame.variationName, newFrame);
			result.variationButtons.get(newFrame.variationName).addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					newFrame.setVisible(true);
				}
			});
		}
		return result;
	}

	

}
