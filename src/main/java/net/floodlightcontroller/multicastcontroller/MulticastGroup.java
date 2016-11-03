package net.floodlightcontroller.multicastcontroller;

import net.floodlightcontroller.devicemanager.internal.AttachmentPoint;

import org.projectfloodlight.openflow.protocol.OFBucket;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActions;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFPort;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class MulticastGroup {
	
	protected static AtomicInteger OFGroupIDGen = new AtomicInteger(1);
	protected AttachmentPoint srcAP;
	/* Add locks for groupMembers */
	protected ConcurrentHashMap<String, AttachmentPoint> groupMembers;
	protected String groupIP;
	protected int destUDPPort;
	protected String sourceIP;
	protected QoSLBMulticastTree mcTree;
	protected MulticastController mcObject;
	protected Logger logger;
	
	protected long bandwidth;
	
	/* Openflow group table specific data structure */
	protected int ofGroupId;
	private ConcurrentHashMap<DatapathId, ArrayList<OFBucket>> ofBucketListMap;
	private ConcurrentHashMap<DatapathId, HashMap<Short, OFBucket>> ofBucketMapMap;

	public AttachmentPoint getSrcAP() {
		return srcAP;
	}

	public void setSrcAP(AttachmentPoint srcAP) {
		this.srcAP = srcAP;
	}
	
	MulticastGroup(String groupIP, int destUDPPort, String scrIP, 
			AttachmentPoint srcAP, MulticastController mc) {
		groupMembers = new ConcurrentHashMap<>();
		this.groupIP = groupIP;
		mcObject = mc;
		this.srcAP = srcAP;
		addGroupSource(scrIP, srcAP, destUDPPort);
		logger = mcObject.logger;
		ofGroupId = OFGroupIDGen.getAndIncrement();
		ofBucketListMap = new ConcurrentHashMap<>();
		ofBucketMapMap = new ConcurrentHashMap<>();
		mcTree = new QoSLBMulticastTree(scrIP , srcAP, this.mcObject, this);
		bandwidth = mc.flowQoSDemandHelper.getQoSBandwidth(groupIP);
	}

	MulticastGroup(String groupIP, MulticastController mc) {
		groupMembers = new ConcurrentHashMap<>();
		this.groupIP = groupIP;
		mcObject = mc;
		logger = mcObject.logger;
		ofGroupId = OFGroupIDGen.getAndIncrement();
		ofBucketListMap = new ConcurrentHashMap<>();
		ofBucketMapMap = new ConcurrentHashMap<>();
		mcTree = null;
		sourceIP = null;
	}

	public void addGroupSource(String srcIP, AttachmentPoint srcAP, int destUDPPort) {
		boolean pathFound;
		this.destUDPPort = destUDPPort;
		this.sourceIP = srcIP;
		this.srcAP = srcAP;
		mcTree = new QoSLBMulticastTree(srcIP, srcAP, this.mcObject, this);		
		for (Entry<String, AttachmentPoint> ent: groupMembers.entrySet()) {
			pathFound = mcTree.addMemberInTheTree(ent.getKey(), ent.getValue(), bandwidth);
		}
	}
	
	public void addMember(String memberIP, AttachmentPoint ap) {
		boolean pathFound = false;
		/*
		 * TODO: This should be access with lock.
		 * Add locks for groupMembers
		 */
		if (groupMembers.containsKey(memberIP)) {
			logger.warn(memberIP + " already exists in  " + groupIP);
			return;
		}
		groupMembers.put(memberIP, ap);
		if (mcTree == null)
			return;
		/*
		 * Source exists and mcTree object is not null
		 * adding a member in the tree
		 */
		pathFound = mcTree.addMemberInTheTree(memberIP, ap, bandwidth);
		if (!pathFound)
			logger.info("MulticastGroup:"+this.groupIP+" Path not found for IP:"
					+memberIP+" bandwidth:"+Long.toString(this.bandwidth));
	}

	public void addMemberOFBucket(DatapathId did, Short portNum, OFBucket ofBucket) {
		ArrayList<OFBucket> ofBucketList = ofBucketListMap.get(did);
		HashMap<Short, OFBucket> ofBucketMap = ofBucketMapMap.get(did);
		ofBucketList.add(ofBucket);
		ofBucketMap.put(portNum, ofBucket);
	}
	
	public OFBucket removeMemberOFBucket(DatapathId did, OFPort ofPort) {
		HashMap<Short, OFBucket> ofBucketMap = ofBucketMapMap.get(did);
		OFBucket ofb = ofBucketMap.get(ofPort.getShortPortNumber());
		ofBucketListMap.get(did).remove(ofb);
		return ofb;
	}
	
	public ArrayList<OFBucket> getMemberOFBucketList(DatapathId did) {
		return ofBucketListMap.get(did);
	}

	public void createMemberOFBucketList(DatapathId did) {
		ofBucketListMap.put(did, new ArrayList<OFBucket>());
		ofBucketMapMap.put(did, new HashMap<Short, OFBucket>());
	}
	
	public void removeMember(String memberIP, AttachmentPoint ap) {
		logger.info("Group:"+this.groupIP + " removeMember: " + memberIP);
		AttachmentPoint pap = groupMembers.get(memberIP);
		if (pap != null && 
			pap.getPort().getPortNumber() == ap.getPort().getPortNumber() && 
			pap.getSw() == ap.getSw()) {
			groupMembers.remove(memberIP);
			if (sourceIP != null)
				mcTree.removeMemberFromTheTree(ap);
			logger.info("Group:"+this.groupIP + " member with IP {} removed successfully", memberIP);
			if (groupMembers.size() == 0) {
				/* TODO:  */
			}
		} else {
			logger.error("Attachment point mismatch with member " +
					"with IP {} while removing", memberIP);
		}

	}

	public void removeMulticastTree() {
		mcTree.deleteMulticastTree();
	}
	
	@Override
	public String toString() {
		return "MulticastGroup [clientIPs=" + groupMembers.toString() + ",contentID="
				+ groupIP + ",destUDPPort="
				+ destUDPPort + ",serviceProviderIP="
				+ sourceIP + "]";
	}
}
