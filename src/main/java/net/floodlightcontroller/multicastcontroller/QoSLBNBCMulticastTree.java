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

import com.google.common.util.concurrent.AtomicDouble;

import ch.qos.logback.classic.filter.ThresholdFilter;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.web.serializers.DPIDSerializer;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.internal.AttachmentPoint;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.linkdiscovery.internal.LinkInfo;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.statistics.SwitchPortBandwidth;
import net.floodlightcontroller.topology.NodePortTuple;

public class QoSLBNBCMulticastTree extends MulticastTree {
	
	/* Link load threshold in percentage, use higher cost route adter this*/
	public static AtomicDouble LinkLoadThreshold = new AtomicDouble(20.0);
	public static double DeltaThreshold = 20.0;
	int noOfLinks = 0;

	TopologyHelper topologyHelper = null;
	
	/*
	 * Stores network topology
	 */
	Map<DatapathId, Set<Link>> networkSwitchLinks;
	
	/*
	 * stores tree branches to the destinations
	 * Hiding super class member
	 */
	protected ConcurrentHashMap<Long, Path> pathsForDestinations;
	
	private static class QueueComparator implements Comparator<PriorityQueue<Path>> {
		@Override
		public int compare(PriorityQueue<Path> o1, PriorityQueue<Path> o2) {
			return o2.size() - o1.size();
		}
	}
	
	/* Stores directional link bandwidth*/

	
	/* Data structures to compute linkBandwidth */
	Map<NodePortTuple, Set<Link>> nodePortLinkMap = null;
	Map<NodePortTuple, SwitchPortBandwidth> switchPortBwMap = null;
	
	QoSLBNBCMulticastTree(String IP, AttachmentPoint srcAP,
			MulticastController mc, MulticastGroup group) {
		super(IP, srcAP, mc, group);
		memberPorts = new ConcurrentHashMap<>();
		topologyHelper = mc.topologyHelper;
		networkSwitchLinks = linkDiscService.getSwitchLinks();
		pathsForDestinations = new ConcurrentHashMap<Long, Path>();
	}
	
	/*
	 * Adds member in the multicast group 
	 */
	@Override
	public boolean addMemberInTheTree(String IP, AttachmentPoint ap, long bandwidth) {
		
		boolean treeNodeReached = false;
		LinkParameter linkParameter = null;
		double initialNetworkLoad = totalInitialLinkLoad.get();
		double newLinkLoad = 0 ;
		long newLinkUtil = 0;
		int newLength = 0;
		double newNBCScore = 0;
		
		destinationDevices.add(ap);
		int currentPathLength = Integer.MAX_VALUE;
		PriorityQueue<Path> pathQueuePrimary = new PriorityQueue<>();
		PriorityQueue<Path> pathQueueSecondary = new PriorityQueue<>();
		PriorityQueue<Path> pathQueueCurrent = null;
		QueueComparator qComp = new QueueComparator();
		PriorityQueue<PriorityQueue<Path>> queues = new PriorityQueue<PriorityQueue<Path>> (qComp);
		
		HashSet<Link> incomingLinks = null;
		Path p = new Path(initialNetworkLoad);
		p.setDstNodePort(ap.getSw(), ap.getPort());
		pathQueuePrimary.add(p);
		Path newPath = null;
		queues.add(pathQueuePrimary);
		HashMap<Long, Integer> visitedLengthMap = new HashMap<>();
		
		HashMap<Long, Path> nodePathMap = new HashMap<>();
		nodePathMap.put(ap.getSw().getLong(), p);
		
		while (queues.peek() != null) {
//			logger.info("-----Polling the queue-----");
			pathQueueCurrent = queues.poll();
//			logger.info("The queue size : " + pathQueueCurrent.size());
			while (pathQueueCurrent.peek() != null) {
				p = pathQueueCurrent.poll();
//				logger.info("---------Current Queue while loop-----------------------");
//				logger.info("Current Path : " + p.toString());	
				DatapathId endNodeId = p.getEndNodeId();
				int i = 0;
				for (Link pl : p.getPathLinks()) {
					visitedLengthMap.put(pl.getDst().getLong(), i++);
				}
				if (isTreeNode(endNodeId)) {
					treeNodeReached = true;
//					logger.info(" End node "+endNodeId.toString()+" is reached ");
					break;
				}
				if (treeNodeReached && currentPathLength <= p.getPathLength()) {
//					logger.info("--------- Continue 1 ---------");
					continue;
				}
//				if (visitedLengthMap.containsKey(p.getEndNodeId().getLong())) {
////					logger.info(p.getEndNodeId().toString() + " is already visited.. ");
////					logger.info("--------- Continue 2 ---------");
//					continue;
//				}
				/* Adding end node of path in to the list of explored node*/
				visitedLengthMap.put(endNodeId.getLong(), p.getPathLength());
				incomingLinks = getIncomingLinks(endNodeId, p.getEndNodePort());				
				for (Link inLink : incomingLinks) {
//					logger.info("---------Link-----------------------");
//					logger.info(inLink.toString());
					if(p.isLoop(inLink) || visitedLengthMap.get(inLink.getSrc().getLong()) != null) {
//						logger.info("--------- loop --continue ---------");
						continue;
					}
					
					linkParameter = linkBandwidth.get(inLink);
//					logger.info("--------- link parameter ---------");
//					logger.info(linkParameter.toString());
//					logger.info("----------------------------------");
					// total capacity check
					newLinkUtil = linkParameter.utilization + bandwidth;
//					logger.info("--newLinkUtil--"+ newLinkUtil);
					if (linkParameter.capacity > newLinkUtil) {
						newLinkLoad = ((double)newLinkUtil/(double)linkParameter.capacity) * 100;
//						logger.info("--newLinkLoad--"+ newLinkLoad);
						if (newLinkLoad < LinkLoadThreshold.get()) {
							newLength = p.getPathLength() + 1;
							newNBCScore = p.getNBCScore() + topologyHelper.getNodeBetweennessCentrality(inLink.getSrc().getLong());
							Path currentPath = nodePathMap.get(inLink.getSrc().getLong());
							if(currentPath != null) {
								if (currentPath.getPathLength() < newLength || 
										(currentPath.getPathLength() == newLength && currentPath.getNBCScore() > newNBCScore )){
//									logger.info("Continuing ");
									continue;
								}
//								logger.info("Removing path " + currentPath.toString());
								pathQueueCurrent.remove(currentPath);
								nodePathMap.remove(currentPath);
							}
							newPath = p.clone();
							newPath.extendPath(inLink, newNBCScore);
							pathQueueCurrent.add(newPath);
							nodePathMap.put(newPath.getEndNodeId().getLong(), newPath);
							if (isTreeNode(newPath.getEndNodeId())) {
//								logger.info("--Destination reached-- "+ newPath.toString());
								currentPathLength = newPath.getPathLength();
								treeNodeReached = true;
							}
						} else {
//							logger.info("adding into secondary queue");
							pathQueueSecondary.add(p);
						}
					}
				}
//				logger.info("---------End of inner for loop---------------------");
			}
//			logger.info("pathQueueSecondary size : " + pathQueueSecondary.size());
			
			if (treeNodeReached) {
//				logger.info("Path found : " + p.toString());
				break;
			}
			if (pathQueueSecondary.size() == 0)
				break;
			queues.add(pathQueueSecondary);
			visitedLengthMap.clear();
//			visitedLengthMap.put(ap.getSw().getLong(), 0);
			pathQueueSecondary = pathQueueCurrent;
			Double tVal = LinkLoadThreshold.addAndGet(DeltaThreshold);
			logger.info("QoSLBMulticastTree:Threshold-Increased:"+Double.toString(tVal)+" " +multicastGroup.groupIP+" IP:"+IP);
		}
		if (!treeNodeReached) {
			logger.error("QoSLBNBCMulticastTree:Group:"+this.multicastGroup.groupIP+" No path found for member IP: " + IP);
			return false;
		}
		if (!pathsForDestinations.containsKey(ap.getSw().getLong())) {
			pathsForDestinations.put(ap.getSw().getLong(), p);
		}
		logger.info("QoSLBNBCMulticastTree:Group:"+this.multicastGroup.groupIP+" Path for IP:" + IP+ " path "+p.toString());
		pushQoSFlowForTreePath(p, ap, bandwidth);
		logger.info("QoSLBNBCMulticastTree:Group="+this.multicastGroup.groupIP+",NumOfRecivers="+destinationDevices.size()+",TreeSize="+treeUpink.size()); 
		return true;
	}
	

	private void pushQoSFlowForTreePath(Path p, AttachmentPoint destAp, long bandwidth) {
		ArrayList<Link> pathLinks = p.getPathLinks();
		addLinkUtilsToMap(pathLinks, bandwidth);
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
//			logger.info("QoSLBNBCMulticastTree:Group:"+this.multicastGroup.groupIP+ " group add 1 " + destAp.getSw().toString() );
		}
		
		Iterator<Link> iter = pathLinks.iterator();
		int size = pathLinks.size();
		for (int i = 0; i < size-1; i++) {
			l = iter.next();
			super.sendOFGroupAddMsg(l.getSrc(), l.getSrcPort(), bandwidth);
			ports = new HashSet<>();
			ports.add(l.getSrcPort());
			memberPorts.put(l.getSrc().getLong(), ports);
//			logger.info("QoSLBNBCMulticastTree:Group:"+this.multicastGroup.groupIP+ " group add 2 " + l.getSrc().toString());
		}
		
		OFPort ofp = p.getEndNodePort();
		HashSet<OFPort> mems = memberPorts.get(p.getEndNodeId().getLong());
		if (mems == null) {
			mems = new HashSet<>();
			super.sendOFGroupAddMsg(p.getEndNodeId(), ofp, bandwidth);
			memberPorts.put(p.getEndNodeId().getLong(), mems);
//			logger.info("QoSLBNBCMulticastTree:Group:"+this.multicastGroup.groupIP+ " group add 3 " + p.getEndNodeId().toString());
		} else {
			super.sendOFGroupModAddMemberMsg(p.getEndNodeId(), ofp, bandwidth);
//			logger.info("QoSLBNBCMulticastTree:Group:"+this.multicastGroup.groupIP+ " group member add " + p.getEndNodeId().toString());
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

//		short pbranchNodeShortNumber = 0;
		IOFSwitch nodeSW = null;
		ArrayList<DatapathId> deleteDpIds = new ArrayList<>();
		ArrayList<Link> links = new ArrayList<>();
		DatapathId nodeId = ap.getSw();
		OFPort memberPort = ap.getPort();

		HashSet<OFPort> memPorts = null;
		Link upl = null;
		/* node id can not be null, it should be at least srcAP id*/
		while(nodeId.getLong() != sourceAP.getSw().getLong()) {
			memPorts = memberPorts.get(nodeId.getLong());
//			pbranchNodeShortNumber = memberPort.getShortPortNumber();
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
		logger.info("QoSLBNBCMulticastTree:Group:"+this.multicastGroup.groupIP + " Delete Path " + dids);

		nodeSW = switchService.getSwitch(nodeId);
		if (nodeId.getLong() == sourceAP.getSw().getLong()) {
			memPorts = memberPorts.get(sourceAP.getSw().getLong());
			memPorts.remove(memberPort);
			if (memPorts.size() == 0) {
				sendOFFlowModDropMulticastStream(nodeSW, false);
				pushOFDeleteGroup(nodeSW);
				memberPorts.remove(sourceAP.getSw().getLong());
//				logger.info("QoSLBNBCMulticastTree:Group:"+this.multicastGroup.groupIP + 
//						" Group Flow deleted 1 from datapath id " + sourceAP.getSw().toString());
			} else{
				sendOFGroupModDelMemberMsg(nodeSW, memberPort);
//				logger.info("QoSLBNBCMulticastTree:Group:"+this.multicastGroup.groupIP + 
//						" Group Member deleted 1 from datapath id " + sourceAP.getSw().toString());
			}
		} else {
			sendOFGroupModDelMemberMsg(nodeSW, memberPort);
//			logger.info("QoSLBNBCMulticastTree:Group:"+this.multicastGroup.groupIP + 
//					" Group Member deleted 2 from datapath id " + nodeId.toString());
		}
		
		ListIterator<DatapathId> iter = deleteDpIds.listIterator(deleteDpIds.size());
		for ( ;iter.hasPrevious();) {
			nodeId= iter.previous();
			nodeSW = switchService.getSwitch(nodeId);
			sendOFFlowModDelForGroup(nodeSW);
			pushOFDeleteGroup(nodeSW);
			treeUpink.remove(nodeId.getLong());
			memberPorts.remove(nodeId.getLong());
//			logger.info("QoSLBNBCMulticastTree:Group:"+this.multicastGroup.groupIP + 
//					" Group Flow deleted 2 from datapath id " + nodeId.toString());
		}
	}
	
	
//	public static void initializeLinkBandwidthMap(ILinkDiscoveryService lds, TopologyHelper topologyHelper) {
//
//		long capacity;
//		Map<Link, LinkInfo> allLinks = lds.getLinks();
//		totalInitialLinkLoad.set(0);
//		logger.info("----Total links in map : "+Integer.toString(allLinks.keySet().size())+"--------");
//		String links = "";
//		for (Link l : allLinks.keySet()) {
//			links += l.toKeyString() + ",";
//			capacity = topologyHelper.getLinkCapacity(l.getSrc().getLong(), l.getDst().getLong());
//			linkBandwidth.put(l, new LinkParameter(capacity, 0));
//		}
//		logger.info(links);
//	}
//
//	public static synchronized void addLinkUtilsToMap(ArrayList<Link> links, long bandwidth) {
//		Double u = 0.0;
//		for (Link l : links) {
//			LinkParameter lp =  linkBandwidth.get(l);
//			 u += lp.addUtilization(bandwidth);
//		}
//		totalInitialLinkLoad.addAndGet(u);
//	}
//	
//	public static synchronized void removeLinkUtilsToMap(ArrayList<Link> links, long bandwidth) {
//		Double u = 0.0;
//		for (Link l : links) {
//			LinkParameter lp =  linkBandwidth.get(l);
//			 u += lp.removeUtilization(bandwidth);
//		}
//		totalInitialLinkLoad.addAndGet(-u);
//	}
	
//	private void updateCurrentBandwidthUtilization() {
//
//		long capacity, util;
//		SwitchPortBandwidth switchPortBandwidth = null;
//		NodePortTuple keyNPT = null;
//		
//		Set<Link> linkPair = null;
//		switchPortBwMap = mcObject.statisticsService.getBandwidthConsumption();
//		nodePortLinkMap = linkDiscService.getPortLinks();
////		mcObject.statisticsService.collectStatistics(true);
//
////		logger.info("--------------");
////		logger.info("switchPortBwMap : " + switchPortBwMap.toString());
////		logger.info("--------------");
////		
////		logger.info("--------------");
////		logger.info("NodePortLinkMap : " + nodePortLinkMap.toString());
////		logger.info("--------------");
////		
////		logger.info("--------------");
////		logger.info("switchPortBwMap size : " + switchPortBwMap.size());
////		logger.info("NodePortLinkMap size : " + nodePortLinkMap.size());
////		logger.info("--------------");
//
////		for (NodePortTuple x : nodePortLinkMap.keySet()) {
////			logger.info(x.toString());	
////		}
////		logger.info("------updateCurrentBandwidthUtilization-------");
//		
//		for (Entry<NodePortTuple, SwitchPortBandwidth> spb: switchPortBwMap.entrySet()) {
//			keyNPT = spb.getKey();
////			logger.info("--------------");
////			logger.info("keyNPT : " + keyNPT.toString());
////			logger.info("--------------");
//			switchPortBandwidth = spb.getValue();
//			linkPair = nodePortLinkMap.get(keyNPT);
//			if (linkPair == null) continue;
////			logger.info("--------------");
////			logger.info("linkPair size : " + linkPair.size());
////			logger.info("--------------");
//			for (Link l : linkPair) {
//				if (l.getSrc().getLong() == switchPortBandwidth.getSwitchId().getLong()) {
//					capacity = topologyHelper.getLinkCapacity(l.getSrc().getLong(), l.getDst().getLong());
//					/*converting bits/s into kb/s*/
//					util = switchPortBandwidth.getBitsPerSecondTx().getValue()/1024;
//					if (util < 0) {
//						logger.error("Negative bandwidth for link: " + l.toKeyString() + " " + Long.toString(util));
//					}
//					linkBandwidth.put(l, new LinkParameter(capacity, util));
////					logger.info("Link=" + l.toKeyString() + ",LinkParameter=" +linkBandwidth.toString());
//				}
////				if (l.getDst().getLong() == switchPortBandwidth.getSwitchId().getLong()) {
////				capacity = topologyHelper.getLinkCapacity(l.getSrc().getLong(), l.getDst().getLong());					
////					/*converting bits/s into kb/s*/
////					util = switchPortBandwidth.getBitsPerSecondRx().getValue()/1000;
//////					linkBandwidth.put(l, new LinkParameter(capacity, util));
////					logger.info("Link=" + l.toKeyString() + ",LinkParameter=" +linkBandwidth.toString());
////				}
//			}
//		}
////		logger.info("----------------------------------------------");
//	}
	
	private double getCurrentTotalLinkLoad() {
		double sum = 0;
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
		
		final int totalLinksIntheNetwork = QoSLBNBCMulticastTree.this.noOfLinks;
		
		private HashSet<Long> dpIds;
		
		/* length of the partially connected path */
		private int pathLength;
		
		/*
		 * if the path is selected, the flow should be directed
		 * towards the port in the node-port tuple
		 */
		private ArrayList<Link> pathLinks;
		
		private double nbcScore;
		
		private DatapathId endNodeId;
		
		private OFPort endNodePort;

		public Path(double totalNetworkLoad) {
			pathLinks = new ArrayList<>();
			dpIds = new HashSet<Long> ();
			nbcScore = 0;
		}
		
		public boolean isLoop(Link l) {
			return dpIds.contains(l.getSrc().getLong());
		}
		
		public void extendPath(Link l, double nbcs) {
			pathLength++;
			pathLinks.add(l);
			endNodeId = l.getSrc();
			endNodePort = l.getSrcPort();
			nbcScore += nbcs;
		}
		
		public void setDstNodePort (DatapathId destDpId, OFPort ofPort) {
			endNodeId = destDpId;
			endNodePort = ofPort;
			dpIds.add(destDpId.getLong());
		}

		@Override
		public int compareTo(Path object) {
			if (this.pathLength != object.pathLength)
				return this.pathLength - object.pathLength;
//			if (this.totalDeviation != object.totalDeviation)
//					return (int)((this.totalDeviation - object.totalDeviation)*1000000);
			return (int)((object.nbcScore - this.nbcScore)*1000000);
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
			
			str += endNodeId.toString().substring(len-2)  + "]";
//					" Port id : " +endNodePort.toString()+"]";
			return str;
		}
		
		public double getNBCScore() {
			return nbcScore;
		}
		
		public int getPathLength() {
			return this.pathLinks.size();
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
	}

}
