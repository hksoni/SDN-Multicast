package net.floodlightcontroller.packet;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.projectfloodlight.openflow.types.IPv4Address;

public class IGMP  extends BasePacket {
	protected byte version;
	protected byte type;
	protected byte maxRespTime;
	protected short checksum;
	protected int groupAddress;
	
	/*IGMPV3 specifics*/
	protected Short noOfGroupRecords;
	protected byte recordType;
	protected byte auxDataLength;
	protected Short noOfSources;
	protected ArrayList<Integer> sources;
	
	public static final byte IGMP_MEMBERSHIP_QUERY = 0x11;
	public static final byte IGMPv1_MEMBERSHIP_REPORT = 0x12;
	public static final byte IGMPv2_MEMBERSHIP_REPORT = 0x16;
	public static final byte IGMPv3_MEMBERSHIP_REPORT = 0x22;
	public static final byte IGMPv2_LEAVE_GROUP = 0x17;
	
	
	public static final String IGMP_QUERY = "IGMP_MEMBERSHIP_QUERY";
	public static final String IGMPV1_REPORT = "IGMPv1_MEMBERSHIP_REPORT";
	public static final String IGMPV2_REPORT = "IGMPv2_MEMBERSHIP_REPORT";
	public static final String IGMPV3_REPORT = "IGMPv3_MEMBERSHIP_REPORT";
	public static final String IGMPV2_LEAVE = "IGMPv2_LEAVE_GROUP";
	
	public byte getVersion() {
		return version;
	}
	
	public byte getType() {
		return this.type;
	}
	
	public String getTypeString() {
		switch (this.type) {
			case IGMP_MEMBERSHIP_QUERY:
				return IGMP_QUERY;
			case IGMPv1_MEMBERSHIP_REPORT:
				return IGMPV1_REPORT;
			case IGMPv2_MEMBERSHIP_REPORT:
				return IGMPV2_REPORT;
			case IGMPv3_MEMBERSHIP_REPORT:
				return IGMPV3_REPORT;
			case IGMPv2_LEAVE_GROUP:
				return IGMPV2_LEAVE;
			default:
				return "unknown type";
		}
		
	}
	
	public IPv4Address getGroupAddress() {
		return IPv4Address.of(groupAddress);
	}

	@Override
	public byte[] serialize() {
		// TODO: Implement
		return null;
	}

	@Override
	public IPacket deserialize(byte[] data, int offset, int length)
			throws PacketParsingException {
		ByteBuffer bb = ByteBuffer.wrap(data, offset, length);
        this.type = bb.get();
        this.maxRespTime = bb.get();
        this.checksum = bb.getShort();
        
        if (type == IGMPv3_MEMBERSHIP_REPORT) {
        	noOfGroupRecords = bb.getShort();
            noOfGroupRecords = bb.getShort();
            noOfSources = bb.getShort();
            this.groupAddress = bb.getInt();
            sources = new  ArrayList<>(noOfSources);
            for (int i=0; i< noOfSources; i++) {
            	sources.add(bb.getInt());
            }
            for (int i=0; i< auxDataLength; i++) {
            	bb.getInt();
            }
        } else {
            this.groupAddress = bb.getInt(); 
        }

        this.payload = new Data();
        int remLength = bb.limit()-bb.position();
        this.payload = payload.deserialize(data, bb.position(), remLength);
        this.payload.setParent(this);
        return this;
	}
	
	@Override
	public String toString(){
		return "[type="+getTypeString()+
				",maxRespTime="+Byte.toString(maxRespTime)+
				",checksum="+Short.toString(checksum)+
				"groupaddress="+getGroupAddress().toString()+"]";
	}
}
