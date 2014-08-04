package poke.server.management.managers;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class MoocChannel {

	private String host;
	private int port;
	private ChannelFuture channel; // do not use directly call connect()!
	private EventLoopGroup group;
	private MoocHandler handler;
	protected static Logger logger = LoggerFactory.getLogger("connect");
	
	public MoocChannel(String host, int port){
		this.host = host;
		this.port = port;

		init();
	}
	
	private void init() {
		// the queue to support client-side surging
		//outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();

		group = new NioEventLoopGroup();
		try {
			handler = new MoocHandler();
			Bootstrap b = new Bootstrap();
			
			
			//ManagementInitializer ci=new ManagementInitializer(false);
			
			MoocInitializer mi=new MoocInitializer(handler, false);
			
			
			//ManagemenetIntializer ci = new ManagemenetIntializer(handler, false);
			//b.group(group).channel(NioSocketChannel.class).handler(handler);
			b.group(group).channel(NioSocketChannel.class).handler(mi);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			// Make the connection attempt.
			channel = b.connect(host, port).syncUninterruptibly();

			// want to monitor the connection to the server s.t. if we loose the
			// connection, we can try to re-establish it.
			ChannelClosedListener ccl = new ChannelClosedListener(this);
			channel.channel().closeFuture().addListener(ccl);
			System.out.println(" channle is "+channel);
		} catch (Exception ex) {
			logger.error("failed to initialize the client connection", ex);

		}

		// start outbound message processor
	//	worker = new OutboundWorker(this);
	//	worker.start();
	}
	
	 
		protected Channel connect() {
			// Start the connection attempt.
			if (channel == null) {
				init();
			}
			System.out.println("channel ===   "+channel);
			if (channel.isDone() && channel.isSuccess())
				return channel.channel();
			else
				throw new RuntimeException("Not able to establish connection to server");
		}
	
	public void addListener(MoocListener listener) {
		// note: the handler should not be null as we create it on construction

		try {
			 logger.info("add listener in MoocChannel is called   and its "+handler);
			handler.addListener(listener);
		} catch (Exception e) {
			logger.error("failed to add listener", e);
		}
	}

	
	public static class ChannelClosedListener implements ChannelFutureListener {
		MoocChannel mc;

		public ChannelClosedListener(MoocChannel mc) {
			this.mc = mc;
		}

		@Override
		public void operationComplete(ChannelFuture arg0) throws Exception {
			// TODO Auto-generated method stub
			
		}

	}
	
}
