package net.floodlightcontroller.multicastcontroller;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

public class FlowQoSDemandHelper {

	HashMap<String, Long> mapping = null;
	
	FlowQoSDemandHelper(String ipBwMappingFileName){
		mapping = new HashMap<String, Long> ();
		getMulticastIPQoSFromFile (ipBwMappingFileName);
		System.out.println(mapping.toString());
	}
	
	public long getQoSBandwidth(String ip) {
		System.out.println(" getQoSBandwidth : " + ip);
		Long  bw = mapping.get(ip);
		if (bw == null)
			bw = Long.valueOf(1000);
		return bw.longValue();
	}
	
	private void getMulticastIPQoSFromFile (String ipBwMappingFileName) {
		FileInputStream fs;
		try {
			fs = new FileInputStream(ipBwMappingFileName);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs));
			String ip = null;
			String line = null;
			String bandwidth = null;
			String[] pair = null;
			while ((line = br.readLine()) != null) {
				pair = line.split("-");
				if (pair.length != 2) {
					System.out.println("error in file named - "+ipBwMappingFileName + " at line : --" +line);
					System.out.println("error in reading file named - "+ipBwMappingFileName);
					br.close();
					break;
				}				
				ip = pair[0].trim();
				bandwidth = pair[1].trim();

				mapping.put(ip, Long.parseLong(bandwidth));
			}
			br.close();
		} catch (Exception e) {
			System.out.println("Exception thrown: error in reading file named - "+ipBwMappingFileName);
		}
	}
}
