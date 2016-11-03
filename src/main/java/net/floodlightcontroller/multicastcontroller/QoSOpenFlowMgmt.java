package net.floodlightcontroller.multicastcontroller;

import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.devicemanager.IDevice;

import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFPort;

public class QoSOpenFlowMgmt {
	
	protected MulticastController mcObject;
	
	QoSOpenFlowMgmt(MulticastController mc) {
		mcObject = mc;
	}
	
	public int pushFlow(DatapathId switchId, OFPort outPort, long bandwidth) {
		return 0;
	}

	private void pushCreateGroupFlow(IOFSwitch sw, OFPort destPort, IDevice destDevice, long bandwidth) {
		
	}

}
