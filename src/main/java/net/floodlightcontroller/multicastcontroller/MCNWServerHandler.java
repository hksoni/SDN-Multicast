package net.floodlightcontroller.multicastcontroller;

import org.projectfloodlight.openflow.types.DatapathId;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class MCNWServerHandler extends ChannelInboundHandlerAdapter {
	
	private ByteBuf buf;
	MulticastController multicastController;
	public MCNWServerHandler(MulticastController mc) {
		multicastController = mc;
	}
	
	 @Override
	 public void handlerAdded(ChannelHandlerContext ctx) {
		 buf = ctx.alloc().buffer(8);
	 }
	 
	 @Override
	 public void handlerRemoved(ChannelHandlerContext ctx) {
		 buf.release();
		 buf = null;
	 }
	    
	 @Override
	 public void channelRead(ChannelHandlerContext ctx, Object msg) {
		 ByteBuf m = (ByteBuf) msg;
		 buf.writeBytes(m);
		 m.release();
		 if (buf.readableBytes() == 8) {
			 DatapathId did = DatapathId.of(buf.readLong());
			 if (multicastController.mcnwClientMap.containsKey(did)) {
				 multicastController.logger.error("Datapath ID sent by MCNWClient already exist \n {}", ctx.toString());
			 } else {
				 multicastController.mcnwClientMap.put(did, new MCNWClientContext(did, ctx, multicastController));
			 }
			 multicastController.logger.info("datapath id {} received ", did.toString());
			 buf.release();
		 }
	 }

	
	 @Override
	 public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		 multicastController.logger.error("error in  {}", cause.toString());
		 ctx.close();
	 }
}
