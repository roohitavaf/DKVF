package edu.msu.cse.dkvf.clusterManager;

/* 
* SSHManager
* 
* @author cabbott
* @version 1.0
*/

import com.jcraft.jsch.*;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SSHManager {
	private static final Logger LOGGER = Logger.getLogger(SSHManager.class.getName());
	private JSch jschSSHChannel;
	private String strUserName;
	private String strConnectionIP;
	private int intConnectionPort;
	private String strPassword;
	private Session sesConnection;
	private int intTimeOut;
	private ChannelSftp channelSftp;

	private void doCommonConstructorActions(String userName, String password, String connectionIP, String knownHostsFileName) throws JSchException {
		jschSSHChannel = new JSch();

		jschSSHChannel.setKnownHosts(knownHostsFileName);

		strUserName = userName;
		strPassword = password;
		strConnectionIP = connectionIP;
	}

	public SSHManager(String userName, String password, String connectionIP, String knownHostsFileName) throws JSchException {
		doCommonConstructorActions(userName, password, connectionIP, knownHostsFileName);
		intConnectionPort = 22;
		intTimeOut = 60000;
	}

	public SSHManager(String userName, String password, String connectionIP, String knownHostsFileName, int connectionPort) throws JSchException {
		doCommonConstructorActions(userName, password, connectionIP, knownHostsFileName);
		intConnectionPort = connectionPort;
		intTimeOut = 60000;
	}

	public SSHManager(String userName, String password, String connectionIP, String knownHostsFileName, int connectionPort, int timeOutMilliseconds) throws JSchException {
		doCommonConstructorActions(userName, password, connectionIP, knownHostsFileName);
		intConnectionPort = connectionPort;
		intTimeOut = timeOutMilliseconds;
	}

	public void setPrivateKey(String keyAddress) throws JSchException {
		 jschSSHChannel.addIdentity(keyAddress);
	}

	public void connect(boolean strictHostKeyChecking) throws JSchException {
		sesConnection = jschSSHChannel.getSession(strUserName, strConnectionIP, intConnectionPort);
		sesConnection.setPassword(strPassword);
		// UNCOMMENT THIS FOR TESTING PURPOSES, BUT DO NOT USE IN PRODUCTION
		if (!strictHostKeyChecking)
			sesConnection.setConfig("StrictHostKeyChecking", "no");
		else
			sesConnection.setConfig("StrictHostKeyChecking", "yes");
		sesConnection.connect(intTimeOut);
		try {
			sesConnection.sendKeepAliveMsg();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Channel channel = sesConnection.openChannel("sftp");
		channel.connect();
		channelSftp = (ChannelSftp) channel;
	}

	private String logError(String errorMessage) {
		if (errorMessage != null) {
			LOGGER.log(Level.SEVERE, "{0}:{1} - {2}", new Object[] { strConnectionIP, intConnectionPort, errorMessage });
		}

		return errorMessage;
	}

	private String logWarning(String warnMessage) {
		if (warnMessage != null) {
			LOGGER.log(Level.WARNING, "{0}:{1} - {2}", new Object[] { strConnectionIP, intConnectionPort, warnMessage });
		}

		return warnMessage;
	}

	public String sendCommandGetResponse(String command) throws JSchException, IOException {
		StringBuilder outputBuffer = new StringBuilder();

		Channel channel = sesConnection.openChannel("exec");
		((ChannelExec) channel).setCommand(command);
		InputStream commandOutput = channel.getInputStream();
		channel.connect();
		int readByte = commandOutput.read();

		while (readByte != 0xffffffff) {
			outputBuffer.append((char) readByte);
			readByte = commandOutput.read();
		}

		channel.disconnect();

		return outputBuffer.toString();
	}

	public void sendCommand(String command) throws JSchException {
		Channel channel = sesConnection.openChannel("exec");
		((ChannelExec) channel).setCommand(command);
		channel.connect();
		channel.disconnect();
	}

	public void uploadFile(String sourceFile, String destinationFile) throws JSchException, SftpException {
		channelSftp.put(sourceFile, destinationFile, ChannelSftp.OVERWRITE);
	}

	public void changePermission(int permission, String fileAddress) throws SftpException {
		channelSftp.chmod(permission, fileAddress);
	}

	public void changePermissionDirectoryContent(int permission, String dir) throws SftpException {
		Vector<LsEntry> list = channelSftp.ls(dir);
		for (LsEntry entry : list) {
			String fileName = entry.getFilename();
			if (fileName.equals(".") || fileName.equals(".."))
				continue;
			String path = dir + "/" + entry.getFilename();
			changePermission(permission, path);
		}
	}

	public void downloadFile(String sourceFile, String destinationFile) throws JSchException, SftpException, IOException {
		InputStream file = channelSftp.get(sourceFile);
		final Path destination = Paths.get(destinationFile);
		Files.copy(file, destination, StandardCopyOption.REPLACE_EXISTING);
	}

	public void makedir(String dir) throws SftpException, JSchException {
		try {
			channelSftp.stat(dir);
		} catch (Exception e) {
			channelSftp.mkdir(dir);
		}
	}

	public boolean doesExist(String file) throws JSchException {
		try {
			channelSftp.stat(file);
			channelSftp.exit();
			return true;
		} catch (Exception e) {
			channelSftp.exit();
			return false;
		}
	}

	public void uploadDirectory(String sourceDirectory, String destinationDirectory) throws JSchException, SftpException {
		makedir(destinationDirectory);
		uploadDirectoryContent(sourceDirectory, destinationDirectory);
	}

	public void uploadDirectoryContent(String sourceDirectory, String destinationDirectory) throws JSchException, SftpException {
		File folder = new File(sourceDirectory);
		File[] listOfFiles = folder.listFiles();
		if (listOfFiles != null) {
			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					uploadFile(listOfFiles[i].getPath(), destinationDirectory + "/" + listOfFiles[i].getName());
				} else if (listOfFiles[i].isDirectory()) {
					uploadDirectory(listOfFiles[i].getPath(), destinationDirectory + "/" + listOfFiles[i].getName());
				}
			}
		}
	}

	public void downloadDirectoryContent(String sourceDirectory, String destinationDirectory) throws SftpException, JSchException, IOException {
		Vector<LsEntry> list = channelSftp.ls(sourceDirectory);
		for (LsEntry entry : list) {
			String fileName = entry.getFilename();
			if (fileName.equals(".") || fileName.equals(".."))
				continue;
			String path = sourceDirectory + "/" + entry.getFilename();
			if (channelSftp.stat(path).isDir()) {
				downloadDirectory(path, destinationDirectory + "/" + fileName);
			} else {
				downloadFile(sourceDirectory + "/" + entry.getFilename(), destinationDirectory + "/" + entry.getFilename());
			}
		}
	}

	public void downloadDirectory(String sourceDirectory, String destinationDirectory) throws JSchException, SftpException, IOException {
		File directory = new File(destinationDirectory);
		if (!directory.exists()) {
			directory.mkdir();
		}
		downloadDirectoryContent(sourceDirectory, destinationDirectory);
	}

	public void cleanDir(String dir) throws JSchException, SftpException {
		deleteDir(dir);
		makedir(dir);
	}

	public void delete(String file) throws JSchException, SftpException {
		try {
			if (channelSftp.stat(file).isDir()) {
				deleteDir(file);
			} else {
				deleteFile(file);
			}
		} catch (SftpException e) {
			if (e.id != ChannelSftp.SSH_FX_NO_SUCH_FILE)
				throw e;
		}
	}

	public void deleteFile(String file) throws JSchException, SftpException {
		try {
			channelSftp.rm(file);
		} catch (SftpException e) {
			if (e.id != ChannelSftp.SSH_FX_NO_SUCH_FILE)
				throw e;
		}
	}

	public void deleteDir(String dir) throws JSchException, SftpException {
		try {

			Vector<LsEntry> list = channelSftp.ls(dir);
			for (LsEntry entry : list) {
				String fileName = entry.getFilename();
				if (fileName.equals(".") || fileName.equals(".."))
					continue;
				String path = dir + "/" + entry.getFilename();
				if (channelSftp.stat(path).isDir()) {
					deleteDir(path);
				} else {
					deleteFile(path);
				}
			}
			channelSftp.rmdir(dir);
		} catch (SftpException e) {
			if (e.id != ChannelSftp.SSH_FX_NO_SUCH_FILE)
				throw e;
		}
	}

	public void disconnect() {
		sesConnection.disconnect();
	}

	public boolean isConnected() {
		return sesConnection.isConnected();
	}

}