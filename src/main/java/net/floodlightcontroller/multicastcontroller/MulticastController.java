package net.floodlightcontroller.multicastcontroller;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.projectfloodlight.openflow.protocol.*;
import org.projectfloodlight.openflow.protocol.OFAggregateStatsReply.Builder;
import org.projectfloodlight.openflow.protocol.action.OFActions;
import org.projectfloodlight.openflow.protocol.actionid.OFActionId;
import org.projectfloodlight.openflow.protocol.actionid.OFActionIds;
import org.projectfloodlight.openflow.protocol.bsntlv.OFBsnTlv;
import org.projectfloodlight.openflow.protocol.bsntlv.OFBsnTlvs;
import org.projectfloodlight.openflow.protocol.errormsg.OFErrorMsgs;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructions;
import org.projectfloodlight.openflow.protocol.instructionid.OFInstructionId;
import org.projectfloodlight.openflow.protocol.instructionid.OFInstructionIds;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.protocol.match.MatchFields;
import org.projectfloodlight.openflow.protocol.meterband.OFMeterBands;
import org.projectfloodlight.openflow.protocol.oxm.OFOxms;
import org.projectfloodlight.openflow.protocol.queueprop.OFQueueProp;
import org.projectfloodlight.openflow.protocol.queueprop.OFQueuePropMaxRate;
import org.projectfloodlight.openflow.protocol.queueprop.OFQueuePropMinRate;
import org.projectfloodlight.openflow.protocol.queueprop.OFQueueProps;
import org.projectfloodlight.openflow.protocol.ver13.OFFactoryVer13;
import org.projectfloodlight.openflow.protocol.ver13.OFQueuePropertiesSerializerVer13;
import org.projectfloodlight.openflow.types.*;
import org.python.tests.mro.ConfusedOnGetitemAdd;

import net.floodlightcontroller.packet.*;
import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.PortChangeType;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.internal.ISwitchDriverRegistry;
import net.floodlightcontroller.core.internal.OFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.web.serializers.DPIDSerializer;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.internal.AttachmentPoint;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery.LDUpdate;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.staticflowentry.IStaticFlowEntryPusherService;
import net.floodlightcontroller.statistics.IStatisticsService;
import net.floodlightcontroller.topology.ITopologyListener;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.topology.NodePortTuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;

import edu.umd.cs.findbugs.annotations.NoWarning;

public class MulticastController implements IOFMessageListener, ILinkDiscoveryListener,
											IOFSwitchListener, IFloodlightModule {
	final short MCNWServerPort = 8833;
	protected IFloodlightProviderService floodlightProvider;
	protected Logger logger;
	
	protected ILinkDiscoveryService lds;
	protected IStatisticsService statisticsService;
	protected IOFSwitchService switchService;
	protected IRoutingService routingService;
	protected IStaticFlowEntryPusherService flowPusher;	
	protected MulticastGroupStore groupDB;
	
	/*----- part of the code for multicast implementation with local agent-----*/
	protected HashSet<MulticastGroupWithLocalAgent> multicastGroupSet = new HashSet<>();
	protected HashMap<DatapathId, MCNWClientContext > mcnwClientMap = new HashMap<>();
	/*---------------------------*/
	
	protected HashMap<DatapathId, OVSDBContext > ovsdbChannelMap = new HashMap<>();
	protected QoSOpenFlowMgmt qosOpenFlowMgmt = new QoSOpenFlowMgmt(this); 
	protected TopologyHelper topologyHelper;
	
	FlowQoSDemandHelper flowQoSDemandHelper = null;
	protected boolean intializedBandwidths = false;
	
	@Override
	public String getName() {
		return MulticastController.class.getSimpleName();
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l =
		        new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(ITopologyService.class);
		l.add(ILinkDiscoveryService.class);
		l.add(IOFSwitchService.class);
		l.add(IDeviceService.class);
		l.add(IRoutingService.class);
		l.add(IStaticFlowEntryPusherService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
	    logger = LoggerFactory.getLogger(MulticastController.class);
		lds = context.getServiceImpl(ILinkDiscoveryService.class);
		routingService = context.getServiceImpl(IRoutingService.class);
		flowPusher = context.getServiceImpl(IStaticFlowEntryPusherService.class);
		switchService = context.getServiceImpl(IOFSwitchService.class);
		statisticsService = context.getServiceImpl(IStatisticsService.class);
		groupDB = new MulticastGroupStore(this);
		Map<String, String> configOptions = context.getConfigParams(this);
		String topologyFile = configOptions.get("topologyFile");
		String ipBandwidthQoSFile = configOptions.get("ipBandwidthQoSFile");
		if (topologyFile != null) {
			topologyHelper = new TopologyHelper(topologyFile);
		} else {
			logger.error("topologyFile not provided");
		}
		
		logger.info(" QOS file : " + ipBandwidthQoSFile);
		flowQoSDemandHelper = new FlowQoSDemandHelper(ipBandwidthQoSFile);
		if (statisticsService.getBandwidthConsumption() == null)
			statisticsService.collectStatistics(false);
	}
	
	private void initNetworkControlServer(int port) {
		EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        logger.info("calling initNetworkControlServer ");
		try {
			final ServerBootstrap bootstrap = new ServerBootstrap();
			bootstrap.group(bossGroup, workerGroup);
			bootstrap.channel(NioServerSocketChannel.class);
			bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new MCNWServerHandler(MulticastController.this));
                }
            });
			//bootstrap.option(ChannelOption.SO_BACKLOG, 128);
			//bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
			bootstrap.childOption(ChannelOption.SO_REUSEADDR, true);
			
		    bootstrap.bind(new InetSocketAddress(port)).sync();
        } catch (Exception e) {
        	logger.error(e.getMessage());
        }
	}
	
	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
		floodlightProvider.addOFMessageListener(OFType.FLOW_REMOVED, this);
		switchService.addOFSwitchListener(this);
		//initNetworkControlServer(MCNWServerPort);

		logger.info("MulticastController startup finished");
		lds.addListener(this);
	}

	/*
	 * UDP packets with source port ranging 10000 to 15000 as 
	 * and 20000 to 25000  destination port will be considered as multicast session
	 * and they need to be sent to 10.1.1.1
	 * Need to resolve ARP here, for this IP
	 */
	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		switch(msg.getType()) {
		case PACKET_IN: {
			OFPacketIn pinMessage = (OFPacketIn)msg;
			Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, 
					IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
			OFPort inPort = pinMessage.getMatch().get(MatchField.IN_PORT);

			/* Various getters and setters are exposed in Ethernet */
			MacAddress srcMac = eth.getSourceMACAddress();
			/* 
			 * Check the ethertype of the Ethernet frame and retrieve the appropriate payload.
			 * Note the shallow equality check. EthType caches and reuses instances for valid types.
			 */
			if (eth.getEtherType() == EthType.IPv4) {
				IPv4 ipv4 = (IPv4) eth.getPayload();
				IPv4Address dstIp = ipv4.getDestinationAddress();
				IPv4Address srcIp = ipv4.getSourceAddress();

				if (ipv4.getProtocol().equals(IpProtocol.IGMP)) {
					/* IGMP packet is received */
//					logger.info( "IGMP packet received with SrcIP {} DstIP {}", srcIp.toString(), dstIp.toString());
					IGMP igmp = (IGMP) ipv4.getPayload();
//					logger.info(igmp.toString());
					handlePacketInWithIGMP(igmp, srcIp, dstIp, sw.getId(), inPort);
					return Command.STOP;
				
				}
				if (dstIp.isMulticast() && ipv4.getProtocol().equals(IpProtocol.UDP)) {
					/* IP packet with multicast IP and UDP dest port */
					/* A multicast source has been found */
					UDP udp = (UDP) ipv4.getPayload();
					
					TransportPort srcPort = udp.getSourcePort();
					TransportPort dstPort = udp.getDestinationPort();
					
					/* Add the multicast group in  groupDB */
					groupDB.addMulticastGroup(dstIp.toString(), dstPort.getPort(), srcIp.toString(), 
							new AttachmentPoint(sw.getId(), inPort, new java.util.Date()));
					return Command.STOP;
				}
				return Command.CONTINUE;
			}
			else {
				return Command.CONTINUE;
			}
		}
		case FLOW_REMOVED:{
			OFFlowRemoved flowRemovedMessage = (OFFlowRemoved)msg;
			U64 cookie = flowRemovedMessage.getCookie();
			String groupIp = IPv4Address.of((int)cookie.getValue()).toString();
			DatapathId did = sw.getId();
			if (flowRemovedMessage.getReason() == 0) {
			logger.warn("Timeout : Flow Removed message from Datapath id " +
					did + " for group ip " + groupIp);
			}
//			logger.warn(flowRemovedMessage.toString());
//			groupDB.removeGroup(groupIp, did);			
		}
		default:
			return Command.CONTINUE;	
		}
	}
	
	private void handlePacketInWithIGMP (IGMP igmpHeader, IPv4Address scrIP, 
			IPv4Address dstIP, DatapathId dpid, OFPort inport) {
		
		switch(igmpHeader.getType()) {
		
		case IGMP.IGMP_MEMBERSHIP_QUERY: {
			/*IGMP with this type is not expected*/
			break;
		}
		case IGMP.IGMPv2_MEMBERSHIP_REPORT: {
//			logger.info("IGMPv2 membership report received");
			this.groupDB.addGroupMember(igmpHeader.getGroupAddress().toString(), 
					scrIP.toString(), 
					new AttachmentPoint(dpid, inport, new java.util.Date()));
			break;
		}
		case IGMP.IGMPv2_LEAVE_GROUP: {
//			logger.info("IGMPv2 LEAVE GROUP received");
			this.groupDB.removeGroupMember(igmpHeader.getGroupAddress().toString(), 
					scrIP.toString(), 
					new AttachmentPoint(dpid, inport, new java.util.Date()));
			break;
		}
		case IGMP.IGMPv3_MEMBERSHIP_REPORT: {
			logger.info("ToDO: IGMPv3_MEMBERSHIP_REPORT received");
			break;
		}
		default:
			logger.info("Unsuppoerted IGMP type");
		}
	}
	
	/*
	 * This function checks if MCNWClient of all the OFSwitch in the network are connected.
	 */
//	private boolean checkMCNWClientConnections () {
//		for (DatapathId did : switchService.getAllSwitchDpids()){
//			if (!mcnwClientMap.containsKey(did)) {
//				logger.warn("MCNWClient of datapathid {} is not connected", did.toString());
//				return false;
//			}
//		}
//		return true;
//	}

	@Override
	public void switchAdded(DatapathId switchId) {
		IOFSwitch ofswitch = switchService.getSwitch(switchId);
		String dpId = switchId.toString();
		InetSocketAddress sadd = (InetSocketAddress) ofswitch.getInetAddress();
		String dpOvsdbIp = sadd.getAddress().getHostAddress();
		String port = topologyHelper.getManagementPort(switchId.getLong());
		logger.info("Datapath ID = "+ switchId.toString() +" "+ switchId.getLong()+ " mgmt port = " + port);
		OVSDBContext ovsDbCt = new OVSDBContext(dpId, dpOvsdbIp, port, this.switchService);
		ovsdbChannelMap.put(switchId, ovsDbCt);
	}

	@Override
	public void switchRemoved(DatapathId switchId) {
		ovsdbChannelMap.remove(switchId);
	}

	@Override
	public void switchActivated(DatapathId switchId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void switchPortChanged(DatapathId switchId, OFPortDesc port,
			PortChangeType type) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void switchChanged(DatapathId switchId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void linkDiscoveryUpdate(LDUpdate update) {
		// TODO Auto-generated method stub
//		logger.info("---------------------------------links : " + lds.getLinks().size());
		if (lds.getLinks().size() == 102 && !intializedBandwidths) {
			QoSLBMulticastTree.initializeLinkBandwidthMap(lds, topologyHelper);
			intializedBandwidths = true;
		}
	}

	@Override
	public void linkDiscoveryUpdate(List<LDUpdate> updateList) {
		// TODO Auto-generated method stub
//		logger.info("---------------------------------links : " + lds.getLinks().size());
		if (lds.getLinks().size() == 102 && !intializedBandwidths) {
			MulticastTree.initializeLinkBandwidthMap(lds, topologyHelper);
			intializedBandwidths = true;
		}
	}

}

/*
 * UDP packets with source port ranging 10000 to 15000 as 
 * and 20000 to 25000  destination port will be considered as multicast session
 * and they need to be sent to 10.1.1.1
 * Need to resolve ARP here, for this IP
 */

/*
 *Multicast code with agent 
 */
//switch(msg.getType()) {
//
//case PACKET_IN:
//	OFPacketIn pinMessage = (OFPacketIn)msg;
//	Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, 
//			IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
//
//	/* Various getters and setters are exposed in Ethernet */
//	MacAddress srcMac = eth.getSourceMACAddress();
//	/* 
//	 * Check the ethertype of the Ethernet frame and retrieve the appropriate payload.
//	 * Note the shallow equality check. EthType caches and reuses instances for valid types.
//	 */
//	if (eth.getEtherType() == EthType.IPv4) {
//		IPv4 ipv4 = (IPv4) eth.getPayload();
//		IPv4Address dstIp = ipv4.getDestinationAddress();
//		IPv4Address srcIp = ipv4.getSourceAddress();
//
//		if (ipv4.getProtocol().equals(IpProtocol.IGMP)) {
//			logger.info( "IGMP packet received with SrcIP {} DstIP {}", srcIp.toString(), dstIp.toString());
//			return Command.STOP;
//
//		} else if (ipv4.getProtocol().equals(IpProtocol.UDP)) {
//			/* We got a UDP packet; get the payload from IPv4 */
//			UDP udp = (UDP) ipv4.getPayload();
//
//			/* Various getters and setters are exposed in UDP */
//			TransportPort srcPort = udp.getSourcePort();
//			TransportPort dstPort = udp.getDestinationPort();
//
//			logger.info("Packet - in data SrcIP="+srcIp.toString()+
//					", DstIP="+dstIp.toString()+", srcport="+
//					Integer.toString(srcPort.getPort())+", dstport="+Integer.toString(dstPort.getPort()));
//
//			OFPort inPort = pinMessage.getMatch().get(MatchField.IN_PORT);
//			MulticastGroup mcg = new MulticastGroup(srcIp.toString(), dstPort.getPort(), srcPort.getPort(), sw, inPort);
//			if(multicastGroupSet.contains(mcg)){
//				logger.info("Udp packet received for already active multicast session : "+mcg.toString());
//				return Command.STOP;
//			}
//			if (!(dstIp.toString().equals("10.0.10.1"))) {
//				logger.info("Random udp traffic: "+mcg.toString());
//				return Command.STOP;
//			}
//			if (!checkMCNWClientConnections()) {
//				logger.error("MCNWClient connectivity isssue");
//				return Command.STOP;
//			}
//			//logger.info("Packet_IN multicast controller...");
//			logger.info("new Group : " + mcg.toString());
//			mcg.createInNetworkMulticastSesion(this);
//			multicastGroupSet.add(mcg);
//			/*
//			 * Multicast source is detected...
//			 * find attachment point, compute RP-Interface of all the nodes
//			 * towards source and send message to multicast agent
//			 */             
//		}
//	}
//	//        if (eth.getEtherType() == EthType.ARP) {
//	//            /* We got an ARP packet; get the payload from Ethernet */
//	//            ARP arp = (ARP) eth.getPayload();
//	//            if (arp.getOpCode() == ARP.OP_REQUEST) {
//	//            	
//	//            }
//	//        }
//	break;
//default:
//	break;
//}



//OFFactory factory = new OFFactoryVer13();
//OFQueueGetConfigRequest cr = factory.buildQueueGetConfigRequest().setPort(OFPort.of(1)).build(); /* Get all queues on all ports */
//ListenableFuture<OFQueueGetConfigReply> future = switchService.getSwitch(switchId).writeRequest(cr); /* Send request to switch 1 */
//try {
//    /* Wait up to 10s for a reply; return when received; else exception thrown */
//    OFQueueGetConfigReply reply = future.get(10, TimeUnit.SECONDS);
//    /* Iterate over all queues */
//    logger.info("Retrieved Queue Size " + Integer.toString(reply.getQueues().size()));
//    for (OFPacketQueue q : reply.getQueues()) {
//    	logger.info(q.toString());
//            OFPort p = q.getPort(); /* The switch port the queue is on */
//            ofswitch.getPort(p);
//            long id = q.getQueueId(); /* The ID of the queue */
//            /* Determine if the queue rates */
//            for (OFQueueProp qp : q.getProperties()) {
//                int rate;
//                /* This is a bit clunky now -- need to improve API in Loxi */
//                switch (qp.getType()) {
//                case OFQueuePropertiesSerializerVer13.MIN_RATE_VAL: /* min rate */
//                    OFQueuePropMinRate min = (OFQueuePropMinRate) qp;
//                    rate = min.getRate();
//                    logger.info("Port No " + Integer.toString(p.getPortNumber()) +
//                    		" Queue ID "+ q.toString() +" Min rate " + Integer.toString(rate));
//                    break;
//                case OFQueuePropertiesSerializerVer13.MAX_RATE_VAL: /* max rate */
//                    OFQueuePropMaxRate max = (OFQueuePropMaxRate) qp;
//                    rate = max.getRate();
//                    logger.info("Port " + Integer.toString(p.getPortNumber())+
//                    		" Queue ID "+ q.toString() +" Max rate " + Integer.toString(rate));
//                    break;
//                }
//            }
//        }
//} catch (Exception e) { /* catch e.g. timeout */
//	e.printStackTrace();
//    logger.error("exception ");
//}
