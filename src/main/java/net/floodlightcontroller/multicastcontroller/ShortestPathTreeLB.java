

package net.floodlightcontroller.multicastcontroller;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFPort;
import org.python.antlr.PythonParser.simple_stmt_return;
import org.python.antlr.ast.Str;
import org.sdnplatform.sync.internal.util.Pair;

import ch.qos.logback.classic.filter.ThresholdFilter;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.web.serializers.DPIDSerializer;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.internal.AttachmentPoint;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;

import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.statistics.SwitchPortBandwidth;
import net.floodlightcontroller.topology.NodePortTuple;

public class ShortestPathTreeLB extends MulticastTree {
	
	
	int noOfLinks = 0;
	ILinkDiscoveryService lds;

	/*
	 * Stores network topology
	 */
	Map<DatapathId, Set<Link>> networkSwitchLinks;
	
	/*
	 * stores tree branches to the destinations
	 * Hiding super class member
	 */
	protected ConcurrentHashMap<Long, Path> pathsForDestinations;
		
	/* Data structures to compute linkBandwidth */
	Map<NodePortTuple, Set<Link>> nodePortLinkMap = null;
	Map<NodePortTuple, SwitchPortBandwidth> switchPortBwMap = null;
	
	TopologyHelper topologyHelper = null;
	
	ShortestPathTreeLB(String IP, AttachmentPoint srcAP,
			MulticastController mc, MulticastGroup group) {
		super(IP, srcAP, mc, group);
		memberPorts = new ConcurrentHashMap<>();
//		linkBandwidth = new HashMap<Link, LinkParameter>();
		topologyHelper = mc.topologyHelper;
		this.lds = mc.lds;
		networkSwitchLinks = lds.getSwitchLinks();
		pathsForDestinations = new ConcurrentHashMap<Long, Path>();
	}
	
	/*
	 * Adds member in the multicast group 
	 */
	@Override
	public  boolean addMemberInTheTree(String IP, AttachmentPoint ap, long bandwidth) {
		
		LinkParameter linkParameter = null;
		switchService.getAllSwitchDpids();
		long newLinkUtil = 0;
		double newLinkLoad = 0;
		double newCost = 0;
		destinationDevices.add(ap);
//		updateCurrentBandwidthUtilization();

		PriorityQueue<Path> queue = new PriorityQueue<Path>();
		HashSet<Link> incomingLinks = null;
		Path p = new Path();
		p.setDstNodePort(ap.getSw(), ap.getPort());
		queue.add(p);
		Path newPath = null;
		HashMap<Long, Path> nodePathMap = new HashMap<>();
		nodePathMap.put(ap.getSw().getLong(), p);
//		logger.info("Source Ap" + sourceAP.getSw().toString());
		if (!isTreeNode(ap.getSw()))
		while (queue.peek() != null) {
			p = queue.poll();
//			logger.info("---------Current path from queue-----------------------");
//			logger.info("Current Path : " + p.toString());	
			DatapathId endNodeId = p.getEndNodeId();
			incomingLinks = getIncomingLinks(endNodeId, p.getEndNodePort());
//			logger.info("Current Node "+ endNodeId.toString() + " " + Double.toString(p.getTotalPathCost()));
//			 logger.info("----- incoming links -----");
//			 logger.info(incomingLinks.toString());
//			 logger.info("--------------------------");
			for (Link inLink : incomingLinks) {
//				 logger.info("---------Link-----------------------");
//				 logger.info(inLink.toString());				
				linkParameter = linkBandwidth.get(inLink);
//				 logger.info("--------- link parameter ---------");
//				 logger.info(linkParameter.toString());
//				 logger.info("----------------------------------");
				newLinkUtil = linkParameter.utilization + bandwidth;
//				 logger.info("--newLinkUtil--"+ newLinkUtil);
				if (linkParameter.capacity > newLinkUtil) {
					newLinkLoad = ((double)newLinkUtil/(double)linkParameter.capacity) * 100;
//					 logger.info("--newLinkLoad--"+ newLinkLoad);
					newCost = p.getTotalPathCost() + newLinkLoad;
					Path currentPath = nodePathMap.get(inLink.getSrc().getLong());
				
					if (currentPath != null) {
						if (currentPath.getTotalPathCost() <= newCost){
//							logger.info("Continuing ");
							continue;
						}
//						logger.info("Removing path " + currentPath.toString());
						queue.remove(currentPath);
						nodePathMap.remove(currentPath);
					}
					newPath = p.clone();
					newPath.extendPath(inLink, newLinkLoad);
//					logger.info("Adding path " + newPath.toString());
					queue.add(newPath);
					nodePathMap.put(newPath.getEndNodeId().getLong(), newPath);
				}
			}
		}
		
		synchronized(this) {
			p = null;
			Path tempPath = null;
			String nodeCost = ""; 
			double minPathCost = Double.MAX_VALUE;
			for (Entry<Long, Path> kv : nodePathMap.entrySet()) {
				tempPath = kv.getValue();
				nodeCost += "["+DatapathId.of(kv.getKey()).toString() + "-" + tempPath.toString()+"]"; 
				if (minPathCost > tempPath.getTotalPathCost() 
						&& sourceAP.getSw().getLong() == kv.getKey()) {
					p = tempPath;
					minPathCost = tempPath.getTotalPathCost();
				}
			}
			if (p != null) {
//				logger.info("ShortestPathTreeLB:Group:"+this.multicastGroup.groupIP+" Node metric for IP:" + IP + " " +nodeCost);
				logger.info("ShortestPathTreeLB:Group:"+this.multicastGroup.groupIP+" Path for IP:" + IP+ " path "+p.toString());
				addLinkUtilsToMap(p.getPathLinks(), bandwidth);
				pushQoSFlowForTreePath(p, ap, bandwidth);
			}
		}
		if (p == null)
			return false;
		logger.info("ShortestPathTreeLB:Group="+this.multicastGroup.groupIP+",NumOfRecivers="+destinationDevices.size()+",TreeSize="+treeUpink.size());
		return true;
	}

	private void pushQoSFlowForTreePath(Path p, AttachmentPoint destAp, long bandwidth) {
		ArrayList<Link> pathLinks = p.getPathLinks();
		Link l = null;
		HashSet<OFPort> ports = null;
		
		for (Iterator<Link> it = pathLinks.iterator(); it.hasNext();){
			Link uplink = it.next();
			treeUpink.put(uplink.getDst().getLong(), uplink);
		}
				
		if (p.getPathLength() != 0) {
			super.sendOFGroupAddMsg(destAp.getSw(), destAp.getPort(), bandwidth);
			ports = new HashSet<>();
			ports.add(destAp.getPort());
			memberPorts.put(destAp.getSw().getLong(), ports);
//			logger.info("ShortestPathTreeLB:Group:"+this.multicastGroup.groupIP+ " group add 1 " + destAp.getSw().toString() );
		}
		
		Iterator<Link> iter = pathLinks.iterator();
		int size = pathLinks.size();
		for (int i = 0; i < size-1; i++) {
			l = iter.next();
			super.sendOFGroupAddMsg(l.getSrc(), l.getSrcPort(), bandwidth);
			ports = new HashSet<>();
			ports.add(l.getSrcPort());
			memberPorts.put(l.getSrc().getLong(), ports);
//			logger.info("ShortestPathTreeLB:Group:"+this.multicastGroup.groupIP+ " group add 2 " + l.getSrc().toString());
		}
		
		OFPort ofp = p.getEndNodePort();
		HashSet<OFPort> mems = memberPorts.get(p.getEndNodeId().getLong());
		if (mems == null) {
			mems = new HashSet<>();
			super.sendOFGroupAddMsg(p.getEndNodeId(), ofp, bandwidth);
			memberPorts.put(p.getEndNodeId().getLong(), mems);
//			logger.info("ShortestPathTreeLB:Group:"+this.multicastGroup.groupIP+ " group add 3 " + p.getEndNodeId().toString());
		} else {
			super.sendOFGroupModAddMemberMsg(p.getEndNodeId(), ofp, bandwidth);
//			logger.info("ShortestPathTreeLB:Group:"+this.multicastGroup.groupIP+ " group member add " + p.getEndNodeId().toString());
		}
		mems.add(ofp);
	}
	
	private boolean isTreeNode(DatapathId did) {
		return (did.getLong() == sourceAP.getSw().getLong() ||
				memberPorts.containsKey(did.getLong()));
	}
	
	private HashSet<Link> getIncomingLinks(DatapathId did, OFPort ofp) {
		Set<Link> links = networkSwitchLinks.get(did);
		HashSet<Link> incomingLinks = new HashSet<>(); 
		for (Link l : links) {
			if (l.getDst().getLong() == did.getLong() && 
				l.getDstPort().getPortNumber() != ofp.getPortNumber() ) {
				incomingLinks.add(l);
			}
		}
		return incomingLinks;
	}
	
	/*
	 * Removes member from the tree
	 */
	@Override
	public void removeMemberFromTheTree(AttachmentPoint ap) {


		IOFSwitch nodeSW = null;
		ArrayList<DatapathId> deleteDpIds = new ArrayList<>();
		DatapathId nodeId = ap.getSw();
		OFPort memberPort = ap.getPort();
		ArrayList<Link> links = new ArrayList<>();
		HashSet<OFPort> memPorts = null;
		Link upl = null;
		/* node id can not be null, it should be at least srcAP id*/
		while(nodeId.getLong() != sourceAP.getSw().getLong()) {
			memPorts = memberPorts.get(nodeId.getLong());
			memPorts.remove(memberPort);
			if (memPorts.size() != 0) {
				break;
			}
			if(upl != null)
				links.add(upl);
			deleteDpIds.add(nodeId);
			upl = treeUpink.get(nodeId.getLong());
			nodeId = upl.getSrc();
			memberPort = upl.getSrcPort();

		}
		removeLinkUtilsToMap(links, this.multicastGroup.bandwidth);
				
		String dids = "[";
		int l = ap.getSw().toString().length();
		for (ListIterator<DatapathId> itr = deleteDpIds.listIterator() ; itr.hasNext() ;) {
			DatapathId d = itr.next();
			dids += d.toString().substring(l-2) + ", ";
		}
		if (nodeId.getLong() != ap.getSw().getLong()) {
			dids += nodeId.toString().substring(l-2);
		}
		dids += " ]";
		logger.info("ShortestPathTreeLB:Group:"+this.multicastGroup.groupIP + " Delete Path " + dids);

		nodeSW = switchService.getSwitch(nodeId);
		if (nodeId.getLong() == sourceAP.getSw().getLong()) {
			memPorts = memberPorts.get(sourceAP.getSw().getLong());
			memPorts.remove(memberPort);
			if (memPorts.size() == 0) {
				sendOFFlowModDropMulticastStream(nodeSW, false);
				pushOFDeleteGroup(nodeSW);
				memberPorts.remove(sourceAP.getSw().getLong());
				logger.info("ShortestPathTreeLB:Group:"+this.multicastGroup.groupIP + 
						" Group Flow deleted 1 from datapath id " + sourceAP.getSw().toString());
			} else{
				sendOFGroupModDelMemberMsg(nodeSW, memberPort);
				logger.info("ShortestPathTreeLB:Group:"+this.multicastGroup.groupIP + 
						" Group Member deleted 1 from datapath id " + sourceAP.getSw().toString());
			}
		} else {
			sendOFGroupModDelMemberMsg(nodeSW, memberPort);
			logger.info("ShortestPathTreeLB:Group:"+this.multicastGroup.groupIP + 
					" Group Member deleted 2 from datapath id " + nodeId.toString());
		}
		
		ListIterator<DatapathId> iter = deleteDpIds.listIterator(deleteDpIds.size());
		for ( ;iter.hasPrevious();) {
			nodeId= iter.previous();
			nodeSW = switchService.getSwitch(nodeId);
			sendOFFlowModDelForGroup(nodeSW);
			pushOFDeleteGroup(nodeSW);
			treeUpink.remove(nodeId.getLong());
			memberPorts.remove(nodeId.getLong());
			logger.info("ShortestPathTreeLB:Group:"+this.multicastGroup.groupIP + 
					" Group Flow deleted 2 from datapath id " + nodeId.toString());
		}
	}
	
	
	private void updateCurrentBandwidthUtilization() {
		long capacity, util;
		SwitchPortBandwidth switchPortBandwidth = null;
		NodePortTuple keyNPT = null;
		
		Set<Link> linkPair = null;
		switchPortBwMap = mcObject.statisticsService.getBandwidthConsumption();
		nodePortLinkMap = linkDiscService.getPortLinks();
		mcObject.statisticsService.collectStatistics(true);

//		logger.info("--------------");
//		logger.info("switchPortBwMap : " + switchPortBwMap.toString());
//		logger.info("--------------");
//		
//		logger.info("--------------");
//		logger.info("NodePortLinkMap : " + nodePortLinkMap.toString());
//		logger.info("--------------");
//		
//		logger.info("--------------");
//		logger.info("switchPortBwMap size : " + switchPortBwMap.size());
//		logger.info("NodePortLinkMap size : " + nodePortLinkMap.size());
//		logger.info("--------------");

//		for (NodePortTuple x : nodePortLinkMap.keySet()) {
//			logger.info(x.toString());	
//		}
//		logger.info("------updateCurrentBandwidthUtilization-------");
		
		for (Entry<NodePortTuple, SwitchPortBandwidth> spb: switchPortBwMap.entrySet()) {
			keyNPT = spb.getKey();
//			logger.info("--------------");
//			logger.info("keyNPT : " + keyNPT.toString());
//			logger.info("--------------");
			switchPortBandwidth = spb.getValue();
			linkPair = nodePortLinkMap.get(keyNPT);
			if (linkPair == null) continue;
//			logger.info("--------------");
//			logger.info("linkPair size : " + linkPair.size());
//			logger.info("--------------");
			for (Link l : linkPair) {
				if (l.getSrc().getLong() == switchPortBandwidth.getSwitchId().getLong()) {
					capacity = topologyHelper.getLinkCapacity(l.getSrc().getLong(), l.getDst().getLong());
					/*converting bits/s into kb/s*/
					util = switchPortBandwidth.getBitsPerSecondTx().getValue()/1024;
					if (util < 0) {
						logger.error("Negative bandwidth for link: " + l.toKeyString() + " " + Long.toString(util));
					}
					linkBandwidth.put(l, new LinkParameter(capacity, util));
//					logger.info("Link=" + l.toKeyString() + ",LinkParameter=" +linkBandwidth.toString());
				}
//				if (l.getDst().getLong() == switchPortBandwidth.getSwitchId().getLong()) {
//				capacity = topologyHelper.getLinkCapacity(l.getSrc().getLong(), l.getDst().getLong());					
//					/*converting bits/s into kb/s*/
//					util = switchPortBandwidth.getBitsPerSecondRx().getValue()/1000;
////					linkBandwidth.put(l, new LinkParameter(capacity, util));
//					logger.info("Link=" + l.toKeyString() + ",LinkParameter=" +linkBandwidth.toString());
//				}
			}
		}
//		logger.info("----------------------------------------------");
	}
	
	private double getCurrentTotalLinkLoad() {
		double sum = 0;
		updateCurrentBandwidthUtilization();
//		logger.info("------getCurrentMeanBandwidthUtilization-------");
		for (Entry<Link, LinkParameter> entry: linkBandwidth.entrySet()) {
			Link link = entry.getKey();
			LinkParameter lp = entry.getValue();
			sum += lp.load;
		}
		noOfLinks = linkBandwidth.size();
//		logger.info("----------------------------------------------");
		return sum;
	}

	
	/*
	 * The path class used to store partially constructed 
	 * path while exploring the graph.   
	 */
	private class Path implements Comparable<Path>, Cloneable{
		
		private HashSet<Long> dpIds;
		
		/* length of the partially connected path */
		private int pathLength;
		
		/*
		 * if the path is selected, the flow should be directed
		 * towards the port in the node-port tuple
		 */
		private ArrayList<Link> pathLinks;
		private Link lastLink;
		private DatapathId endNodeId;
		private OFPort endNodePort;
		private double totalPathCost;

		public double getTotalPathCost() {
			return totalPathCost;
		}

		public Path() {
			totalPathCost = 0;
			pathLinks = new ArrayList<>();
			dpIds = new HashSet<Long> ();
		}
		
		public boolean isLoop(Link l) {
			return dpIds.contains(l.getSrc().getLong());
		}
		
		public void extendPath(Link l, double linkCost) {
			pathLength++;
			pathLinks.add(l);
			lastLink = l;
			endNodeId = l.getSrc();
			endNodePort = l.getSrcPort();
			totalPathCost += linkCost;
		}
		
		public void setDstNodePort (DatapathId destDpId, OFPort ofPort) {
			endNodeId = destDpId;
			endNodePort = ofPort;
			dpIds.add(destDpId.getLong());
		}

		@Override
		public int compareTo(Path object) {
			return (int)((this.totalPathCost - object.totalPathCost) *100000);	
		}
		
		@Override
	    public Path clone() {
			Path pnew = null;
			try {
				pnew = (Path)super.clone();
				pnew.pathLinks = (ArrayList<Link>) this.pathLinks.clone();
				pnew.dpIds = (HashSet<Long>)this.dpIds.clone();

			} catch (CloneNotSupportedException e) {
				logger.info("Could not clone path object");
			}
			return pnew;
	    }
		
		@Override
		public String toString() {
			int len = endNodeId.toString().length();;
			String str = "[";
			for (Link l : pathLinks) {
				str += l.getDst().toString().substring(len-2) + ",";
			}
			
			str += endNodeId.toString().substring(len-2) +"] "  
//					+" Port id : "+endNodePort.toString()
			+ " [ cost=" + Double.toString(totalPathCost)
			+"]";
			return str;
		}
		
		public int getPathLength() {
			return this.pathLinks.size();
		}

		public void setPathLength(int pathLength) {
			this.pathLength = pathLength;
		}

		public Link getLastLink() {
			return lastLink;
		}

		public void setLastLink(Link lastLink) {
			this.lastLink = lastLink;
		}
		
		public DatapathId getEndNodeId() {
			return endNodeId;
		}

		public OFPort getEndNodePort() {
			return endNodePort;
		}

		public ArrayList<Link> getPathLinks() {
			return pathLinks;
		}

		public void setPathLinks(ArrayList<Link> pathLinks) {
			this.pathLinks = pathLinks;
		}
	}
	

}
