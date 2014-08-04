package poke.server.management.managers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.GeneratedMessage;
import eye.Comm.Management;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class MoocHandler extends SimpleChannelInboundHandler<eye.Comm.Management> {

	protected static Logger logger = LoggerFactory.getLogger("Mooc");

	protected ConcurrentMap<String, MoocListener> listeners = new ConcurrentHashMap<String, MoocListener>();
	private volatile Channel channel;
	
	public MoocHandler() {
		logger.info("-----------------------------Mooc Handler initialized--------------------------------");
	}
	
	
	public String getNodeId() {
		if (listeners.size() > 0)
			return listeners.values().iterator().next().getListenerID();
		else if (channel != null)
			return channel.localAddress().toString();
		else
			return String.valueOf(this.hashCode());
	}

	public void addListener(MoocListener listener) {
		if (listener == null)
			return;

		listeners.putIfAbsent(listener.getListenerID(), listener);
	}

	public boolean send(GeneratedMessage msg) {
		// TODO a queue is needed to prevent overloading of the socket
		// connection. For the demonstration, we don't need it
		
		 // Rohit- I am writing write and flush instead of write
		
		ChannelFuture cf = channel.writeAndFlush(msg);
		if (cf.isDone() && !cf.isSuccess()) {
			logger.error("failed to poke!");
			return false;
		}

		return true;
	}

	
	
	@Override
	protected void channelRead0(ChannelHandlerContext arg0, Management msg)
			throws Exception {
		// TODO Auto-generated method stub
		for (String id : listeners.keySet()) {
			MoocListener ml = listeners.get(id);

			// TODO this may need to be delegated to a thread pool to allow
			// async processing of replies
			ml.onMessage(msg);
		}
		
	}
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		logger.error("Mooc channel inactive");
		
		// TODO try to reconnect
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}
