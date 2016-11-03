package net.floodlightcontroller.multicastcontroller;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.staticflowentry.IStaticFlowEntryPusherService;
import net.floodlightcontroller.topology.ITopologyService;

import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.j3d.utils.scenegraph.io.state.com.sun.j3d.utils.geometry.PrimitiveState;


public class MulticastGroupWithLocalAgent implements Comparable<MulticastGroupWithLocalAgent>{

	final short MagicInterfaceNumber = 32767;
	int destUDPPort;
	int srcUDPPort;
	String sourceIP;
	//these need to be added in 
	final static String destIP = "10.1.1.1";
	
	protected ILinkDiscoveryService lds;
	protected IOFSwitchService switchService; 

	protected Logger logger;
	
	/*
	 * The switch connected to the source of the group
	 */
	IOFSwitch rootSwitch;	
	OFPort sourceAttachedPort;


	MulticastGroupWithLocalAgent(String ip, int destPort, int srcPort, IOFSwitch srcSw, OFPort srcAttachPort) {
		destUDPPort = destPort;
		srcUDPPort = srcPort;
		sourceIP = ip;
		rootSwitch = srcSw;
		sourceAttachedPort = srcAttachPort;
//		spTree = new ShortestPathTree(IP, app, this);
	}
	
	void createInNetworkMulticastSesion(MulticastController mc) {
		
		MCNWClientContext mcnwcc;
		Queue<DatapathId> bfsQueue = new  LinkedList<DatapathId>();

		Set<DatapathId> visited = new  HashSet<DatapathId>();
		
		switchService = mc.switchService;
		lds = mc.lds;
		Map<DatapathId, Set<Link>> links = lds.getSwitchLinks();
		
//		mc.logger.info("getSwitchLinks() map  size : " + links.size());		
//		mc.logger.info("getSwitchLinks() map : " + links.toString());
		
		
		bfsQueue.add(rootSwitch.getId());
		
		mcnwcc = mc.mcnwClientMap.get(rootSwitch.getId());
		mcnwcc.sendMulticastSessionRPIMessage(sourceIP, destIP, srcUDPPort, destUDPPort, sourceAttachedPort.getShortPortNumber(), (short)1);
		
		while (!bfsQueue.isEmpty()) {
			DatapathId currentSwitch = bfsQueue.poll();
			
			visited.add(currentSwitch);
			Set<Link> swLinks = links.get(currentSwitch);
			if (swLinks == null) {
				mc.logger.info("swLinks are null");
				continue;
			}
//			mc.logger.info("swLinks for {} are {}", currentSwitch.toString(), swLinks.toString());
			for (Link l :swLinks) {
				DatapathId endDpid = l.getSrc();
				OFPort endDpidPort = l.getSrcPort();
				//The src-dst are compared to find out adjacent endpoint
				if (endDpid.equals(currentSwitch)) {//Add destination switch in the queue
					continue;
				}
				if (visited.contains(endDpid))
					continue;

				bfsQueue.add(endDpid);
				mcnwcc = mc.mcnwClientMap.get(endDpid);
				mcnwcc.sendMulticastSessionRPIMessage(sourceIP, destIP, srcUDPPort, destUDPPort, endDpidPort.getShortPortNumber(), (short)0);
			}
		}
	}
	
	@Override
	public String toString() {
		return "MulticastGroup [ source = " + sourceIP.toString() + ", Src UDP port = "
				+ srcUDPPort + ", Dst UDP port = " + destUDPPort + "Attached Switch/port "
				+ rootSwitch.getId().toString()+"/"+sourceAttachedPort.getShortPortNumber() +"]";
	}

	@Override
	public int compareTo(MulticastGroupWithLocalAgent o) {
		int ipComp = sourceIP.compareTo(o.sourceIP);
		if (ipComp != 0)
			return ipComp;
		return (srcUDPPort + destUDPPort - o.srcUDPPort - o.destUDPPort);
	}
	
    @Override
    public int hashCode() {
        final int msboffset = 0xFF000000;
        return IPv4.toIPv4Address(sourceIP) + msboffset + destUDPPort + srcUDPPort;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
    	MulticastGroupWithLocalAgent arg = (MulticastGroupWithLocalAgent) obj;	
//    	if (this.sourceIP.equals(arg.sourceIP))
    		if ((this.destUDPPort+this.srcUDPPort) == (arg.destUDPPort + arg.srcUDPPort))
    			return true;
    	return false;
    }
}