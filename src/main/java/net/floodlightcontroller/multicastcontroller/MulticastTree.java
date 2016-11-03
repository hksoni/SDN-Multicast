package net.floodlightcontroller.multicastcontroller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.projectfloodlight.openflow.protocol.OFActionType;
import org.projectfloodlight.openflow.protocol.OFBucket;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.OFFlowDelete;
import org.projectfloodlight.openflow.protocol.OFFlowMod;
import org.projectfloodlight.openflow.protocol.OFFlowModFlags;
import org.projectfloodlight.openflow.protocol.OFFlowModify;
import org.projectfloodlight.openflow.protocol.OFGroupDelete;
import org.projectfloodlight.openflow.protocol.OFFlowMod.Builder;
import org.projectfloodlight.openflow.protocol.OFGroupAdd;
import org.projectfloodlight.openflow.protocol.OFGroupMod;
import org.projectfloodlight.openflow.protocol.OFGroupType;
import org.projectfloodlight.openflow.protocol.OFMatchV3;
import org.projectfloodlight.openflow.protocol.OFOxmList;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionGroup;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.action.OFActionSetField;
import org.projectfloodlight.openflow.protocol.action.OFActionSetQueue;
import org.projectfloodlight.openflow.protocol.action.OFActions;
import org.projectfloodlight.openflow.protocol.instruction.OFInstruction;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructionApplyActions;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructionClearActions;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructions;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.oxm.OFOxmEthType;
import org.projectfloodlight.openflow.protocol.oxm.OFOxmIpProto;
import org.projectfloodlight.openflow.protocol.oxm.OFOxmIpv4Dst;
import org.projectfloodlight.openflow.protocol.oxm.OFOxmIpv4Src;
import org.projectfloodlight.openflow.protocol.oxm.OFOxms;
import org.projectfloodlight.openflow.protocol.ver13.OFFactoryVer13;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFGroup;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.U64;

import com.google.common.util.concurrent.AtomicDouble;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.internal.ISwitchDriverRegistry;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.devicemanager.internal.AttachmentPoint;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.linkdiscovery.internal.LinkInfo;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.staticflowentry.IStaticFlowEntryPusherService;
import net.floodlightcontroller.statistics.SwitchPortBandwidth;
import net.floodlightcontroller.topology.NodePortTuple;

public class MulticastTree {

	protected static final short FLOWMOD_DEFAULT_IDLE_TIMEOUT = 10;
//	protected HashMap<Long, HashSet<NodePortTuple>> memberPorts;
	protected ConcurrentHashMap<Long, HashSet<OFPort>> memberPorts;
	
	protected Vector<AttachmentPoint> destinationDevices;
	protected ConcurrentHashMap<Long, Route> pathsForDestinations;
	protected ConcurrentHashMap<Long, Link> treeUpink;
	
	//Source related Info
	protected Integer sourceIP;
	protected AttachmentPoint sourceAP;
	
	protected MulticastGroup multicastGroup;
	protected MulticastController mcObject;
	
	protected IRoutingService routingService;
	protected IOFSwitchService switchService;
	protected IDeviceService deviceService;
	protected static ILinkDiscoveryService linkDiscService;
	protected IFloodlightProviderService floodlightProviderService;
	protected IStaticFlowEntryPusherService flowPusher;
	
	protected static org.slf4j.Logger logger = LoggerFactory.getLogger(MulticastTree.class);

	static ConcurrentHashMap<Link, LinkParameter> linkBandwidth = new ConcurrentHashMap<Link, LinkParameter>();
	static AtomicDouble totalInitialLinkLoad = new AtomicDouble();
	public static synchronized void initializeLinkBandwidthMap(ILinkDiscoveryService lds, TopologyHelper topologyHelper) {

		long capacity;
		Map<Link, LinkInfo> allLinks = lds.getLinks();
		totalInitialLinkLoad.set(0);
		logger.info("NumberOfLinks="+Integer.toString(allLinks.keySet().size()));
//		String links = "";
		for (Link l : allLinks.keySet()) {
//			links += l.toKeyString() + ",";
			capacity = topologyHelper.getLinkCapacity(l.getSrc().getLong(), l.getDst().getLong());
			linkBandwidth.put(l, new LinkParameter(capacity, 0));
		}
//		logger.info(links);
	}

	public static synchronized void addLinkUtilsToMap(ArrayList<Link> links, long bandwidth) {
		Double u = 0.0, tu = 0.0;
		for (Link l : links) {
			LinkParameter lp =  linkBandwidth.get(l);
			tu = lp.addUtilization(bandwidth);
			u += tu;
			if (tu >= 100.0) {
				logger.error("Link capacity exceeded for link : "+l.toKeyString());
			}
		}
		totalInitialLinkLoad.addAndGet(u);
	}
	
	public static synchronized void removeLinkUtilsToMap(ArrayList<Link> links, long bandwidth) {
		Double u = 0.0;
		for (Link l : links) {
			LinkParameter lp =  linkBandwidth.get(l);
			 u += lp.removeUtilization(bandwidth);
		}
		totalInitialLinkLoad.addAndGet(-u);
	}
	
	MulticastTree(String IP, AttachmentPoint srcAP, MulticastController mc, MulticastGroup group) {
		sourceIP = IPv4.toIPv4Address(IP);
		memberPorts = new ConcurrentHashMap<Long, HashSet<OFPort>>();
		destinationDevices = new Vector<AttachmentPoint> ();
		this.mcObject = mc;
		routingService = mc.routingService;
		
		treeUpink = new ConcurrentHashMap<Long, Link>();
		switchService = mc.switchService;
		linkDiscService = mc.lds;
		floodlightProviderService = mc.floodlightProvider;
		flowPusher = mc.flowPusher;
		sourceAP = srcAP;
		pathsForDestinations = new ConcurrentHashMap<Long, Route>();
		multicastGroup = group;
		sendOFFlowModDropMulticastStream(switchService.getSwitch(srcAP.getSw()), true);
	}
	
	/*
	 * Adds member in the multicast group 
	 */
	public boolean addMemberInTheTree(String IP, AttachmentPoint ap, long bamdwidth) {
		Integer destIP = IPv4.toIPv4Address(IP);
		IDevice destDevice = null;
		destinationDevices.add(ap);
		AddFlowsDirectedToDevice(ap, destDevice, bamdwidth);
		return true;
		
//		logger.info("---Bandwidth consuption log--");
//		for (Entry<NodePortTuple, SwitchPortBandwidth> spb: mcObject.statisticsService.getBandwidthConsumption().entrySet()){
//			logger.info("NodePortTuple " + spb.getKey().toString() 
//					+" SwitchPortBandwidth: BitsPerSecondRx = " + Long.toString(spb.getValue().getBitsPerSecondRx().getValue())
//					+ " BitsPerSecondTx" + Long.toString(spb.getValue().getBitsPerSecondTx().getValue())
//					);
//		}
//		
//		logger.info("---Bandwidth consuption log--");
	}

	/*
	 * Removes member from the tree
	 */
	public void removeMemberFromTheTree(AttachmentPoint ap) {

		/* Remove flow from edge switch */
		DatapathId dSWId = ap.getSw();
		IOFSwitch dSW = switchService.getSwitch(dSWId);
		HashSet<OFPort> nps;
		Route r = pathsForDestinations.get(ap.getSw().getLong());
		if (r == null)
			return;
		NodePortTuple from = null;
		List<NodePortTuple> path = r.getPath();
		ListIterator<NodePortTuple> it = path.listIterator(path.size());
		while (it.hasPrevious()) {
			from = it.previous();
			dSWId = from.getNodeId();
			dSW = switchService.getSwitch(dSWId);
			nps = memberPorts.get(dSWId.getLong());
			if (nps == null) {
				logger.info("RemoveMemberFromTheTree: unexpected null nps");
				return;
			}
			nps.remove(from.getPortId());
			if (nps.size() != 0) {
				sendOFGroupModDelMemberMsg(dSW, from.getPortId());
				/* switch has downstream member other than one removed*/
				break;
			}
			memberPorts.remove(dSWId.getLong());
			if (dSWId.getLong() == sourceAP.getSw().getLong())
				sendOFFlowModDropMulticastStream(dSW, false);
			else 
				sendOFFlowModDelForGroup(dSW);
			pushOFDeleteGroup(dSW);
			if (it.hasPrevious())
				from = it.previous();	
		}
		pathsForDestinations.remove(ap.getSw().getLong());
	}
	
	
	/*
	 * Deletes the flow, the group  with required match fields and 
	 * group action from all the OpenFlow13 switch maintaining the state
	 */
	public void deleteMulticastTree() {
		
		IOFSwitch dSW = null;
		for (Entry<Long, HashSet<OFPort>> v : memberPorts.entrySet()){
			dSW = switchService.getSwitch(DatapathId.of(v.getKey()));
			logger.info("Group:"+this.multicastGroup.groupIP+" Datapath id : "+ DatapathId.of(v.getKey()).toString() + " has member ports");
			if (v.getValue().size() > 0) {
				sendOFFlowModDelForGroup(dSW);
				pushOFDeleteGroup(dSW);
			}
		}
	}

	
	private void AddFlowsDirectedToDevice(AttachmentPoint destAP, 
			IDevice destDevice, long bandwidth) {

		NodePortTuple to = null;
		DatapathId toNodeId = null;
		OFPort toOFPort = null;
		HashSet <OFPort> members = null;
		String pathStr = "SPT route added - [";
		int strLen = multicastGroup.srcAP.getSw().toString().length();
		Route r = routingService.getRoute(multicastGroup.srcAP.getSw(), multicastGroup.srcAP.getPort()
				,destAP.getSw(), destAP.getPort(), U64.ZERO);
//		logger.info("Group:"+this.multicastGroup.groupIP + " Route for: Src Datapath :"
//				+multicastGroup.srcAP.getSw().toString().substring(strLen-2)
//				+", port "+multicastGroup.srcAP.getPort().toString()
//				+" to Dst Datapath "+destAP.getSw().toString().substring(strLen-2)
//				+" port "+destAP.getPort().toString()+" ");
//		logger.info(r.toString());	
		pathsForDestinations.put(destAP.getSw().getLong(), r);
		List<NodePortTuple> path = r.getPath();
//		logger.info("Group:"+this.multicastGroup.groupIP+" Route path= "+path.toString());;
		ListIterator<NodePortTuple> it = path.listIterator(path.size());
		for (int i = path.size(); i > 0; i-=2) {
			to = it.previous();
			toNodeId = to.getNodeId();
			toOFPort = to.getPortId();
//			logger.info(" to = " + to.toString());
			members = memberPorts.get(toNodeId.getLong());
			if (members == null) {
				members = new HashSet<OFPort>();
				memberPorts.put(toNodeId.getLong(), members);
				members.add(toOFPort);
//				logger.info("Group:"+this.multicastGroup.groupIP+" sendOFGroupAddMsg: "
//				+ toNodeId.toString().substring(strLen-2)+" "+toOFPort.toString());
				pathStr += toNodeId.toString().substring(strLen-2) + ",";
				sendOFGroupAddMsg(toNodeId, toOFPort, bandwidth);
			} else {
				members.add(toOFPort);
				sendOFGroupModAddMemberMsg(toNodeId, toOFPort, bandwidth);
//				logger.info("Group:"+this.multicastGroup.groupIP+" sendOFGroupModAddMemberMsg: "
//				+ toNodeId.toString().substring(strLen-2)+" "+toOFPort.toString());
				pathStr += toNodeId.toString().substring(strLen-2) + "]";
				logger.info("Group:"+this.multicastGroup.groupIP + " SPT path added - "+ pathStr);
				return;
			}
			to = it.previous();				
		}
		logger.info("Group:"+this.multicastGroup.groupIP + " SPT path added - "+ pathStr);
	}
	
	protected void sendOFGroupModAddMemberMsg(DatapathId switchId,
			OFPort toOFPort, long bandwidth) {
		IOFSwitch sw = switchService.getSwitch(switchId);
		sendOFGroupModAddMemberMsg(sw, toOFPort, bandwidth);
		
	}

	/*
	 * send OF message to switch to create an OF Group with one dowstream member
	 * And, inserts flow with the group actions
	 */
	protected void sendOFGroupAddMsg(IOFSwitch sw, OFPort destPort, long bandwidth) {
		multicastGroup.createMemberOFBucketList(sw.getId());
		addQoSOFBucketToGroup(sw, destPort, bandwidth);
		pushOFCreateGroup(sw);
		if (sw.getId().getLong() != sourceAP.getSw().getLong())
			sendOFFlowModForGroup(sw, true);
		else
			sendOFFlowModForGroup(sw, false);
	}
	
	/*
	 * send OF message to switch to create an OF Group with one dowstream member
	 * And, inserts flow with the group actions
	 */
	protected void sendOFGroupAddMsg(DatapathId switchId , OFPort destPort, long bandwidth) {
		IOFSwitch sw = switchService.getSwitch(switchId);
		sendOFGroupAddMsg(sw, destPort, bandwidth);
	}
	
	/*
	 * Adds a downstream member in the existing OF group
	 */
	protected void sendOFGroupModAddMemberMsg(IOFSwitch sw, OFPort destPort, long bandwidth) {
		addQoSOFBucketToGroup(sw, destPort, bandwidth);
		pushOFModifyGroup(sw);
	}
	
	/* 
	 * This function creates group on openflow switch 
	 * using OF13 protocol 
	 */
	private void pushOFCreateGroup(IOFSwitch sw) {

		/* Creates group flow with set queue option */
		OFFactory of13Factory = sw.getOFFactory();
		
		OFGroupAdd addGroup = of13Factory.buildGroupAdd()
				.setGroup(OFGroup.of(multicastGroup.ofGroupId))
				.setGroupType(OFGroupType.ALL)
				.setBuckets(multicastGroup.getMemberOFBucketList(sw.getId()))
				.build();
		sw.write(addGroup);
	}
	
	/* 
	 * Modify the group with existing bucketlist in multicastgroup 
	 */
	private void pushOFModifyGroup(IOFSwitch sw) {

		/* Creates group flow with set queue option */
		OFFactory of13Factory = sw.getOFFactory();

		OFGroupMod modGroup = of13Factory.buildGroupModify()
				.setGroup(OFGroup.of(multicastGroup.ofGroupId))
				.setGroupType(OFGroupType.ALL)
				.setBuckets(multicastGroup.getMemberOFBucketList(sw.getId()))
				.build();
		
		sw.write(modGroup);
	}

	/* 
	 * Deletes the group along with queues attaches to all the member ports
	 */
	protected void pushOFDeleteGroup(IOFSwitch sw) {
		
		/* Delete the queues attached to all the member ports*/
		long queueId = -1;
		OFPort ofp = null;
		ArrayList<OFBucket> ofBuckets = multicastGroup.getMemberOFBucketList(sw.getId());
		OVSDBContext ovsdbContext = mcObject.ovsdbChannelMap.get(sw.getId());
		for (OFBucket ofb : ofBuckets) {
			queueId = -1;
			ofp = null;
			for (OFAction ofa:  ofb.getActions()){
				if (ofa.getType() == OFActionType.SET_QUEUE){
					OFActionSetQueue ofas = (OFActionSetQueue) ofa;
					queueId = ofas.getQueueId();
					/* removes queue using OVSDBContext on the switch */
					
				}
				if (ofa.getType() == OFActionType.OUTPUT){
					OFActionOutput ofo = (OFActionOutput) ofa;
					ofp = ofo.getPort();
					/* removes queue using OVSDBContext on the switch */
					
				}
			}
			if (ofp != null && queueId != -1)
				ovsdbContext.deleteQueueOnPort(ofp, (int)queueId);
			else
				logger.error("pushOFDeleteGroup: unexpected actionset in group bucket");
		}
		
		/* Creates group flow with set queue option */
		OFFactory of13Factory = sw.getOFFactory();

		OFGroupDelete delGroup = of13Factory.buildGroupDelete()
				.setGroup(OFGroup.of(multicastGroup.ofGroupId))
				.setGroupType(OFGroupType.ALL)
				.build();
		sw.write(delGroup);
	}
	
	/* 
	 * This function adds the bucket for specified destPort 
	 * in the group. It creates QoS queue on the port first.
	 * Then creates set-queue and output OF13 actions on the port. 
	 */
	private void addQoSOFBucketToGroup(IOFSwitch sw, OFPort destPort, long bandwidth) {

		/* creates queue using OVSDBContext on the switch */
		OVSDBContext ovsdbContext = mcObject.ovsdbChannelMap.get(sw.getId());
		
		/* FIXME: hardcoded priority '1' for all flows */
		int queueId = ovsdbContext.createQueueOnPort(destPort, Long.toString(bandwidth), "1");
		
		OFFactory of13Factory = sw.getOFFactory();
		
		/* creates actions for the bucket */
		ArrayList<OFAction> actionList = new ArrayList<OFAction>();
		OFActions actions = of13Factory.actions();
		OFActionSetQueue setQueue = actions.buildSetQueue().setQueueId(queueId).build();
		actionList.add(setQueue);
		OFActionOutput output = actions.buildOutput()
				.setMaxLen(0xFFffFFff)
				.setPort(destPort).build();
		actionList.add(output);
		
		/* creates a bucket */
		OFBucket bucket = of13Factory.buildBucket()
				.setActions(actionList)
				.setWatchGroup(OFGroup.ANY)
				.setWatchPort(OFPort.ANY)
				.build();
		/* store the bucket in multicastGroup object */
		multicastGroup.addMemberOFBucket(sw.getId(), 
				destPort.getShortPortNumber(), bucket);
	}
	
	/* 
	 * This function removes the bucket for specified destPort 
	 * in the group. It creates QoS queue on the port first.
	 */
	protected void sendOFGroupModDelMemberMsg(IOFSwitch sw, OFPort destPort) {
		/* removes member port form the group*/
		OFBucket ofb = multicastGroup.removeMemberOFBucket(sw.getId(), destPort);
		/* prepares a modify object sends it to the switch */
		pushOFModifyGroup(sw);

		long queueId = -1;
		for (OFAction ofa:  ofb.getActions()){
			if (ofa.getType() == OFActionType.SET_QUEUE){
				OFActionSetQueue ofas = (OFActionSetQueue) ofa;
				queueId = ofas.getQueueId();
			}
		}
		/* removes queue using OVSDBContext on the switch */
		OVSDBContext ovsdbContext = mcObject.ovsdbChannelMap.get(sw.getId());
		ovsdbContext.deleteQueueOnPort(destPort, (int)queueId);
	}

	/*
	 * Creates the flow with required match fields and 
	 * group action in OpenFlow13 switch.
	 */
	private void sendOFFlowModForGroup(IOFSwitch sw, boolean add) {
		/* Creates group flow with set queue option */
		OFFactory of13Factory = sw.getOFFactory();
		OFMatchV3 of13Match = buildOFMatchV3();
		
		ArrayList<OFAction> actionList = new ArrayList<OFAction>();
		OFActions actions = of13Factory.actions();
		OFActionGroup ofActionGroup = actions.buildGroup().setGroup(OFGroup.of(multicastGroup.ofGroupId)).build();
		actionList.add(ofActionGroup);
		
		/* creates instruction and adds above actionList in apply action list  */
		OFInstructions insts = of13Factory.instructions();
		OFInstructionApplyActions applyActions = insts.buildApplyActions()
			    .setActions(actionList)
			    .build();
		ArrayList<OFInstruction> instructions = new ArrayList<>();
		instructions.add(applyActions);
		OFFlowMod.Builder ofmBuilder = null;
		OFFlowMod ofm = null;
		if (add){
			ofmBuilder = of13Factory.buildFlowAdd();
		}
		else {
			ofmBuilder = of13Factory.buildFlowModify();
		}
		Set<OFFlowModFlags> offf=  new HashSet<>();
		offf.add(OFFlowModFlags.SEND_FLOW_REM);
		ofm = ofmBuilder.setActions(actionList)
				.setCookie(U64.of(IPv4Address.of(multicastGroup.groupIP).getInt()))
				.setCookieMask(U64.FULL_MASK)
				.setFlags(offf)
				.setBufferId(OFBufferId.NO_BUFFER)
				.setHardTimeout(0)
				.setIdleTimeout(FLOWMOD_DEFAULT_IDLE_TIMEOUT)
				.setPriority(100)
				.setMatch(of13Match)
				.setInstructions(instructions)
				.build();
		sw.write(ofm);
	}
	
	/*
	 * Creates the flow with required match fields and 
	 * group action in OpenFlow13 switch.
	 */
	protected void sendOFFlowModDropMulticastStream(IOFSwitch sw, boolean addFlag) {
		/* Creates group flow with set queue option */
		OFFactory of13Factory = sw.getOFFactory();

		OFMatchV3 of13Match = buildOFMatchV3();
		ArrayList<OFAction> actionList = new ArrayList<OFAction>();
		
		/* creates instruction and adds above actionList in apply action list  */
		OFInstructions insts = of13Factory.instructions();
		OFInstructionClearActions clearActions = insts.clearActions();
		ArrayList<OFInstruction> instructions = new ArrayList<>();
		instructions.add(clearActions);
		
		OFFlowMod.Builder buildFlowMod;
		OFFlowMod flowMod;
				
		if (!addFlag) {
			buildFlowMod = of13Factory.buildFlowModify();
		}
		else {
			buildFlowMod = of13Factory.buildFlowAdd();	
		}
		Set<OFFlowModFlags> offf=  new HashSet<>();
		offf.add(OFFlowModFlags.SEND_FLOW_REM);
		buildFlowMod.setActions(actionList)
		.setCookie(U64.of(IPv4Address.of(multicastGroup.groupIP).getInt()))
		.setCookieMask(U64.FULL_MASK)
		.setFlags(offf)
		.setBufferId(OFBufferId.NO_BUFFER)
		.setHardTimeout(0)
		.setIdleTimeout(FLOWMOD_DEFAULT_IDLE_TIMEOUT)
		.setPriority(100)
		.setMatch(of13Match)
		.setInstructions(instructions);
		flowMod = buildFlowMod.build();
		sw.write(flowMod);
	}
	
	/*
	 * Deletes the flow with required match fields and 
	 * group action in OpenFlow13 switch.
	 */
	protected void sendOFFlowModDelForGroup(IOFSwitch sw) {
		/* Creates group flow with set queue option */
		OFFactory of13Factory = sw.getOFFactory();
		OFMatchV3 of13Match = buildOFMatchV3();
		
		ArrayList<OFAction> actionList = new ArrayList<OFAction>();
		OFActions actions = of13Factory.actions();
		OFActionGroup ofActionGroup = actions.buildGroup().setGroup(OFGroup.of(multicastGroup.ofGroupId)).build();
		actionList.add(ofActionGroup);
		
		/* creates instruction and adds above actionList in apply action list  */
		OFInstructions insts = of13Factory.instructions();
		OFInstructionApplyActions applyActions = insts.buildApplyActions()
			    .setActions(actionList)
			    .build();
		ArrayList<OFInstruction> instructions = new ArrayList<>();
		instructions.add(applyActions);
		Set<OFFlowModFlags> offf=  new HashSet<>();
		offf.add(OFFlowModFlags.SEND_FLOW_REM);
		OFFlowDelete addFlow = of13Factory.buildFlowDelete()
				.setCookie(U64.of(IPv4Address.of(multicastGroup.groupIP).getInt()))
				.setCookieMask(U64.FULL_MASK)
				.setFlags(offf)
				.setActions(actionList)
				.setBufferId(OFBufferId.NO_BUFFER)
				.setHardTimeout(3600)
				.setIdleTimeout(FLOWMOD_DEFAULT_IDLE_TIMEOUT)
				.setPriority(32768)
				.setMatch(of13Match)
				.setInstructions(instructions)
//				.setOutGroup(OFGroup.of(multicastGroup.ofGroupId))
				.build();
		sw.write(addFlow);
	}
	
	private OFMatchV3 buildOFMatchV3() {
		OFFactory of13Factory = OFFactories.getFactory(OFVersion.OF_13);
		OFOxms oxms = of13Factory.oxms();
		
		OFOxmEthType ofOxmEthType = oxms.buildEthType().setValue(EthType.IPv4).build();
		OFOxmIpv4Src ofOxmIpv4Src = oxms.buildIpv4Src().setValue(IPv4Address.of(multicastGroup.sourceIP)).build();
		OFOxmIpProto ofOxmIpProto = oxms.buildIpProto().setValue(IpProtocol.UDP).build();
		OFOxmIpv4Dst ofOxmIpv4Dst = oxms.buildIpv4Dst().setValue(IPv4Address.of(multicastGroup.groupIP)).build();
		OFOxmList oxmList = OFOxmList.of(ofOxmEthType, ofOxmIpv4Src, ofOxmIpv4Dst, ofOxmIpProto);
		OFMatchV3 of13Match = of13Factory.buildMatchV3().setOxmList(oxmList).build();
		return of13Match;		
	}
	
	@Override
	public String toString() {
		return "ShortestPathTree [sourceIP=" + sourceIP + ", destinationDevices ="
				+  destinationDevices + ", sourceAP=" + multicastGroup.srcAP + ", treeLinks="
				+ memberPorts + "]";
	}
}
