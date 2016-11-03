package net.floodlightcontroller.multicastcontroller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * This class executes ovs-vsctl utility on controller node, 
 * connectes to ovsdb-server on remote node to update ovsdb.
 * The main purpose of the class is used to set up queue configurations. 
 */
public class ExecuteOvsVsctl {
	protected static Logger logger = LoggerFactory.getLogger(ExecuteOvsVsctl.class);
private
	String remoteOvsDBArg;

public
	ExecuteOvsVsctl(String ip, String mgmtPort) {
		remoteOvsDBArg = "--db=tcp:"+ip+":"+mgmtPort;
	}

	public String run(String[] commandPart) {
		ArrayList<String> comm = new ArrayList<String>();
		for (int i = 0; i< commandPart.length ; i++) {
			comm.add(commandPart[i]);
		}
		return run(comm);
	}
	
	public String run(ArrayList<String> commandPart) {
		String retVal = null;
		ArrayList<String> commandArgs = new ArrayList<String>();
		
//		commandArgs.add("sudo");
//		commandArgs.add("-S");
		commandArgs.add("ovs-vsctl");
		commandArgs.add(remoteOvsDBArg);
		commandArgs.add("--no-wait");
		
		if (!commandArgs.addAll(commandPart)) {
			logger.error("ovs-vsctl failed to change command args ");
			return null;
		}
		
//		logger.info("Executing: "+commandArgs.toString());
		ProcessBuilder pb = new ProcessBuilder(commandArgs);
		try {
			Process process = pb.start();
			int err = process.waitFor();
			retVal = output(process.getInputStream());
			
			if (err != 0) {
				logger.error("Return value non-zero in executing " + commandArgs.toString());
			}
//			logger.info(retVal);
		} catch (Exception e) {
			logger.error("Exception in executing " + commandArgs.toString());
			return retVal;
		}
		return retVal.trim();
	}
	
	private static String output(InputStream inputStream) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(inputStream));
			String line = null;
			while ((line = br.readLine()) != null) {
				sb.append(line + System.getProperty("line.separator"));
			}
		} finally {
			br.close();
		}
		return sb.toString();
	}

}
