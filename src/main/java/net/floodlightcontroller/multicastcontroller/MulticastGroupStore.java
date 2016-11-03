package net.floodlightcontroller.multicastcontroller;

import java.util.HashMap;

import net.floodlightcontroller.core.web.serializers.DPIDSerializer;
import net.floodlightcontroller.devicemanager.internal.AttachmentPoint;

import org.projectfloodlight.openflow.types.DatapathId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MulticastGroupStore {
	
	/*
	 * Multicast Ip is key
	 */
	/*TODO: Add locks for groupMembers */
	protected HashMap<String, MulticastGroup > multicastGroupDatabase;
	protected MulticastController mcObject;

	protected Logger logger;
	
	public MulticastGroupStore(MulticastController mc) {
		multicastGroupDatabase = new HashMap<String , MulticastGroup >();
		this.mcObject = mc;
		logger = mc.logger;
	}
	
	boolean addMulticastGroup (String groupIP, int destPort, 
			String srcIP, AttachmentPoint srcAP) {
		MulticastGroup mcg = multicastGroupDatabase.get(groupIP); 
		if (mcg == null) {
			mcg  = new MulticastGroup(groupIP, destPort, srcIP, srcAP, mcObject);
			multicastGroupDatabase.put(groupIP, mcg);
			logger.info("AddMulticastGroup : Group added with IP " + groupIP);
			return true;
		}
		else {
			if (mcg.sourceIP == null){
				mcg.addGroupSource(srcIP, srcAP, destPort);
				logger.info("AddMulticastGroup: Source "+ srcIP+" added to group " + groupIP);
			} else {
				logger.error("Group:"+ groupIP + " Group Already Exist " + 
						" AP: "+ srcAP.getSw().toString() + "," + 
						Short.toString(srcAP.getPort().getShortPortNumber()) + " source IP :"+ srcIP);
			}
			return false;
		}
	}
	
	public void addGroupMember (String groupIP, String memberIP, AttachmentPoint ap) {
		MulticastGroup mcGroup = multicastGroupDatabase.get(groupIP);
		if (mcGroup == null) {
			/* Receiver for a absent groups source */
			mcGroup = new MulticastGroup(groupIP, mcObject);
			multicastGroupDatabase.put(groupIP, mcGroup);
			//logger.info("AddMulticastGroup : Group added with IP {} and member IP {} ", groupIP, memberIP);
		}
		//logger.info("AddMulticastGroup : Group added with IP {} and member IP {} ", groupIP, memberIP);
		mcGroup.addMember(memberIP, ap);
	}
	
	boolean removeGroupMember (String groupIP, String memberIP, AttachmentPoint ap) {
		if (!multicastGroupDatabase.containsKey(groupIP)) {
			logger.info("Group with IP {}  does not exist", groupIP);
			return false;
		}
		else {
			/* Add receiver in existing group */
			multicastGroupDatabase.get(groupIP).removeMember(memberIP, ap);
			return true;
		}
	}
	
	boolean removeGroup (String groupIP, DatapathId datapathId) {
		MulticastGroup mcg = multicastGroupDatabase.get(groupIP);
		if (mcg == null) {
			logger.info("Group with IP {}  does not exist", groupIP);
			 multicastGroupDatabase.remove(groupIP);
			return false;
		}
		else {
			if (mcg.getSrcAP().getSw().getLong() == datapathId.getLong()) {
				/* Remove the group and all its receivers */
				multicastGroupDatabase.remove(groupIP);
				mcg.removeMulticastTree();
			}
			return true;
		}
	}
}

