package edu.msu.cse.dkvf.clusterManager;

import java.io.IOException;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

public class TestSSHManagerForAWS {
	public static void main (String[] args) throws JSchException, IOException, SftpException{
		SSHManager manager = new SSHManager("ubuntu", "", "52.90.84.4", "");
		manager.setPrivateKey("keys/NewDKVFKey.pem");
		manager.connect(false);
		manager.makedir("/home/ubuntu/DKVF");
		String result = manager.sendCommandGetResponse("ls /home/ubuntu");
		System.out.println(result);
	}
}
