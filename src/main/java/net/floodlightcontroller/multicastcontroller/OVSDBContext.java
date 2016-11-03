package net.floodlightcontroller.multicastcontroller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;

public class OVSDBContext extends Object {
	
	protected static Logger logger = LoggerFactory.getLogger(MulticastController.class);;
	IOFSwitchService switchService;
	String datapathId;
	String mgmtPort;
	ExecuteOvsVsctl ovs_vsctl_executor;
	private static int QueueIdGen = 0;
	
	/*
	 * Map of port name (string) to queues on the port
	 * queues are hashed by integer queue id
	 */
	HashMap<String, HashMap<Integer, OVSPortQueue>> portQueuesMap;
	
	HashMap<String, String> portQoSMap;
	
	public OVSDBContext(String datapathId, String ip, String port, IOFSwitchService ofss) {
		this.switchService = ofss;
		this.datapathId = datapathId;
		mgmtPort = port;
		ovs_vsctl_executor = new ExecuteOvsVsctl(ip, mgmtPort);
		portQueuesMap = new HashMap<>();
		portQoSMap = new HashMap<>();
		getPortQoSUUIDs();
	}
	
	public int createQueueOnPort(OFPort port, String rate, String priority) {
	    IOFSwitch ofSwitch = switchService.getSwitch(DatapathId.of(this.datapathId));
	    OFPortDesc ofpdsc = ofSwitch.getPort(port);

	    /* creates a queue */
	    OVSPortQueue ovspq = createQueue(rate, priority);
//	    logger.info("createQueueOnPort: "+"PortName="+ofpdsc.getName()
//	    		+"QoS="+portQoSMap.get(ofpdsc.getName())
//	    		+"OFPortNumner="+port.getShortPortNumber()
//	    		+"Queue="+ovspq.toString()
//	    		);
	    
	    /* adds queue to QoS */
	    addQueueToQoS(ofpdsc.getName(), ovspq);
	    
	    /* stores the queue id to OVSPortQueue mapping per for the port */
	    HashMap<Integer, OVSPortQueue> queueMap = portQueuesMap.get(ofpdsc.getName()); 
	    if (queueMap == null) {
	    	queueMap = new HashMap<>();
	    	portQueuesMap.put(ofpdsc.getName(), queueMap);
	    }
	    queueMap.put(ovspq.queueId,ovspq);
	    return ovspq.queueId;
	}
	
	public void deleteQueueOnPort(OFPort port, int queueId) {
	    IOFSwitch ofSwitch = switchService.getSwitch(DatapathId.of(this.datapathId));
	    OFPortDesc ofpdsc = ofSwitch.getPort(port);
	    OVSPortQueue ovspq = portQueuesMap.get(ofpdsc.getName()).get(queueId);
	    removeQueueFromQoS(ofpdsc.getName(), ovspq);
	    
	    destroyQueue(ovspq);
	    HashMap<Integer, OVSPortQueue> queueMap = portQueuesMap.get(ofpdsc.getName());
	    if (queueMap == null) {
	    	logger.error("deleteQueueOnPort: null queueMap is not expected");
	    }
	    queueMap.remove(ovspq.queueId);
	}
	
	private void getPortQoSUUIDs() {
		String uuid = null;
	    IOFSwitch ofSwitch = switchService.getSwitch(DatapathId.of(this.datapathId));
	    Collection<OFPortDesc> ports = ofSwitch.getPorts();
	    for(OFPortDesc ofPDesc : ports) {
	    	String portName = ofPDesc.getName();
	    	String command = "get port "+portName+" qos";
	    	uuid = ovs_vsctl_executor.run(command.split(" "));
	    	portQoSMap.put(portName, uuid);
//			logger.info("Command output : " + command + "  output : "+ uuid);
	    }
//		logger.info("DatapathId="+this.datapathId+",portQoSMap:" + portQoSMap.toString());
	}
	
    /* 
     * sudo ovs-vsctl --db=unix://run/openvswitch/db.sock create queue  
     * other-config:min-rate=2500 other-config:max-rate=2500
     * rate variable should be in Kbps
     */
	private OVSPortQueue createQueue(String rate, String priority) {
		Long r = Long.parseLong(rate) * 1024;
		Long burst = r/10;
		ArrayList<String> command = new ArrayList<String>();
		command.add("create");
		command.add("queue");
		command.add("other-config:min-rate="+r.toString());
		command.add("other-config:max-rate="+r.toString());
		command.add("other_config:priority="+priority);
		command.add("other_config:burst="+Long.toString(burst));
		String queueUUID = ovs_vsctl_executor.run(command);
		if (queueUUID == "") {
			logger.error("QueueUUID is not returned");
		}
		OVSPortQueue ovsq = new OVSPortQueue(queueUUID, ++QueueIdGen, r, r, burst);
		return ovsq;
	}

	private void destroyQueue(OVSPortQueue ovspq) {
		ArrayList<String> command = new ArrayList<String>();
		command.add("destroy");
		command.add("queue");
		command.add(ovspq.queueUUID);
		ovs_vsctl_executor.run(command);
//		String retOutStream = 
	}
	
	private void addQueueToQoS(String portName, OVSPortQueue ovsPQ) {
	    ArrayList<String> command = new ArrayList<String> ();
		command.add("set");
		command.add("qos");
		command.add(portQoSMap.get(portName));
		command.add("queues:"+Integer.toString(ovsPQ.queueId)+"="+ovsPQ.queueUUID);
		String retOutStream = ovs_vsctl_executor.run(command);
		String cs = "";
		for (String s : command) {
			cs += s + " "; 
		}
//		logger.debug(cs +": " + retOutStream );
	}

	private void removeQueueFromQoS(String portName, OVSPortQueue ovsPQ) {
	    ArrayList<String> command = new ArrayList<String> ();
		command.add("remove"); 
		command.add("qos");
		command.add(portQoSMap.get(portName));
		command.add("queues");
		command.add(Integer.toString(ovsPQ.queueId));
		String retOutStream = ovs_vsctl_executor.run(command);
		String cs = "";
		for (String s : command) {
			cs += s + " "; 
		}
//		logger.debug(cs +": " + retOutStream );
	}
	
	private class OVSPortQueue extends Object {
		String queueUUID;
		int queueId;
		long maxRate; //  in Kbit/s
		long minRate; //  in Kbit/s
		long burst; //  in bits
		
		public OVSPortQueue(String queueUUID, int queueId, long maxRate, long minRate, long burst) {
			this.queueId = queueId;
			this.maxRate = maxRate;
			this.minRate = minRate;
			this.burst = burst;
			this.queueUUID = queueUUID;
		}
		
		@Override
		public String toString() {
			return "UUID=" + queueUUID + 
					",id=" + Integer.toString(queueId) +
					",maxRate="+Long.toString(maxRate) +
					",minRate="+Long.toString(minRate) + 
					",burst=" + Long.toString(burst); 	
		}
	}
}
