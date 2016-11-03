package net.floodlightcontroller.multicastcontroller;

import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.slf4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

public class MCNWClientContext {
	final ChannelHandlerContext mcnwClienthandlerCtx;
	DatapathId dpid;
	protected Logger logger;

	public DatapathId getDpid() {
		return dpid;
	}

	public void setDpid(DatapathId dpid) {
		this.dpid = dpid;
	}

	MCNWClientContext(DatapathId id, final ChannelHandlerContext ctx, MulticastController mc) {
		mcnwClienthandlerCtx = ctx;
		dpid = id;
		logger = mc.logger;
	}
	
	public void sendMulticastSessionRPIMessage(String sourceIP, String destip, 
			int srcUDPPort, int destUDPPort, short intf, short isRootSwitch) {
		int sip = IPv4Address.of(sourceIP).getInt();
		int dip = IPv4Address.of(destip).getInt();
		short sup = (short)srcUDPPort;
		short dup = (short)destUDPPort;

		final ByteBuf msg = mcnwClienthandlerCtx.alloc().buffer(16);
		msg.writeInt(sip);
		msg.writeInt(dip);
		msg.writeShort(sup);
		msg.writeShort(dup);
		msg.writeShort(intf);
		msg.writeShort(isRootSwitch);
		final ChannelFuture f = mcnwClienthandlerCtx.writeAndFlush(msg);
		f.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) {
				assert f == future;
				logger.info("Multicast flow and interface message sent to datapath id {}", dpid.toString());
			}
		});
	}
}

