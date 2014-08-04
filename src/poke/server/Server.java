/*
 * copyright 2014, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.LeaderElection;
import eye.Comm.LeaderElection.Builder;
import eye.Comm.LeaderElection.VoteAction;
import eye.Comm.LeaderElectionOrBuilder;
import eye.Comm.Management;
import poke.server.conf.JsonUtil;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.ManagementInitializer;
import poke.server.management.ManagementQueue;
import poke.server.management.managers.ElectionManager;
import poke.server.management.managers.HeartbeatConnector;
import poke.server.management.managers.HeartbeatData;
import poke.server.management.managers.HeartbeatManager;
import poke.server.management.managers.JobManager;
import poke.server.management.managers.MoocHandler;
import poke.server.management.managers.MoocInitializer;
import poke.server.management.managers.NetworkManager;
import poke.server.resources.ResourceFactory;

/**
 * Note high surges of messages can close down the channel if the handler cannot
 * process the messages fast enough. This design supports message surges that
 * exceed the processing capacity of the server through a second thread pool
 * (per connection or per server) that performs the work. Netty's boss and
 * worker threads only processes new connections and forwarding requests.
 * <p>
 * Reference Proactor pattern for additional information.
 * 
 * @author gash
 * 
 */
public class Server {
	protected static Logger logger = LoggerFactory.getLogger("server");

	protected static ChannelGroup allChannels;
	protected static HashMap<Integer, ServerBootstrap> bootstrap = new HashMap<Integer, ServerBootstrap>();


	protected ServerConf conf;

	protected JobManager jobMgr;
	protected NetworkManager networkMgr;
	protected HeartbeatManager heartbeatMgr;
	
	protected ElectionManager electionMgr;
	//Start New Code -By  Date: 03/23/2014
	public static String myId;
	//End New Code -By  Date:03/23/2014
	/**
	 * static because we need to get a handle to the factory from the shutdown
	 * resource
	 */
	public static void shutdown() {
		try {
			if (allChannels != null) {
				ChannelGroupFuture grp = allChannels.close();
				grp.awaitUninterruptibly(5, TimeUnit.SECONDS);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		logger.info("Server shutdown");
		System.exit(0);
	}

	/**
	 * initialize the server with a configuration of it's resources
	 * 
	 * @param cfg
	 */
	public Server(File cfg) {
		init(cfg);
	}

	private void init(File cfg) {
		// resource initialization - how message are processed
		BufferedInputStream br = null;
		try {
			byte[] raw = new byte[(int) cfg.length()];
			br = new BufferedInputStream(new FileInputStream(cfg));
			br.read(raw);
			conf = JsonUtil.decode(new String(raw), ServerConf.class);
			ResourceFactory.initialize(conf);
		} catch (Exception e) {
		}
	}

	public void release() {
		if (HeartbeatManager.getInstance() != null)
			HeartbeatManager.getInstance().release();
	}

	/**
	 * initialize the outward facing (public) interface
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private static class StartCommunication implements Runnable {
		ServerConf conf;

		public StartCommunication(ServerConf conf) {
			this.conf = conf;
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				String str = conf.getServer().getProperty("port");
				if (str == null) {
					// TODO if multiple servers can be ran per node, assigning a
					// default
					// is not a good idea
					logger.warn("Using default port 5570, configuration contains no port number");
					str = "5570";
				}

				int port = Integer.parseInt(str);

				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(port, b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				logger.info("Server Initializer - will now be called from here!");
				b.childHandler(new ServerInitializer(compressComm));

				// Start the server.
				logger.info("Starting server " + conf.getServer().getProperty("node.id") + ", listening on port = "
						+ port);
				//Start New Code -By  Date: 03/23/2014
				final String myId = conf.getServer().getProperty("node.id");
				//End New Code -By  Date: 03/23/2014
				ChannelFuture f = b.bind(port).syncUninterruptibly();

				// should use a future channel listener to do this step
				// allChannels.add(f.channel());

				// block until the server socket is closed.
				f.channel().closeFuture().sync();
			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup public handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();
			}

			// We can also accept connections from a other ports (e.g., isolate
			// read
			// and writes)
		}
	}

	/**
	 * initialize the private network/interface
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private static class StartManagement implements Runnable {
		private ServerConf conf;

		public StartManagement(ServerConf conf) {
			this.conf = conf;
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			// UDP: not a good option as the message will be dropped

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				String str = conf.getServer().getProperty("port.mgmt");
				int mport = Integer.parseInt(str);
				//Start new code 
				myId = conf.getServer().getProperty("node.id");
				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(mport, b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				b.childHandler(new ManagementInitializer(compressComm));

				// Start the server.

				logger.info("Starting mgmt " + conf.getServer().getProperty("node.id") + ", listening on port = "
						+ mport);
				ChannelFuture f = b.bind(mport).syncUninterruptibly();

				// block until the server socket is closed.
				f.channel().closeFuture().sync();
			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup public handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();
			}
		}
	}

	private static class MoocCommunication implements Runnable {
		private ServerConf conf;

		public MoocCommunication(ServerConf conf) {
			this.conf = conf;
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			// UDP: not a good option as the message will be dropped

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				String str = conf.getServer().getProperty("port.mooc");
				logger.info("Mooc port is "+str);
				int mport = Integer.parseInt(str);
				//Start new code 
				myId = conf.getServer().getProperty("node.id");
				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(mport, b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				MoocHandler mhandler = new MoocHandler();
				b.childHandler(new MoocInitializer(mhandler,compressComm));

				logger.info("Starting MOOC Communication " + conf.getServer().getProperty("node.id") + ", listening on port = "
						+ mport);
				ChannelFuture f = b.bind(mport).syncUninterruptibly();

				// block until the server socket is closed.
				f.channel().closeFuture().sync();
			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup public handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();
			}
		}
	}



	/**
	 * this initializes the managers that support the internal communication
	 * network.
	 * 
	 * TODO this should be refactored to use the conf file
	 */
	/**
	 * 
	 */
	private void startManagers() {
		if (conf == null)
			return;

		// start the inbound and outbound manager worker threads
		ManagementQueue.startup();
		//Start New Code  Date: 03/23/2014
		myId = conf.getServer().getProperty("node.id");
		//End New Code  Date: 03/23/2014
		// create manager for network changes
		networkMgr = NetworkManager.getInstance(myId);

		// create manager for leader election
		String str = conf.getServer().getProperty("node.votes");
		int votes = 1;
		if (str != null)
			votes = Integer.parseInt(str);
		electionMgr = ElectionManager.getInstance(myId, votes); // id = nodeid, votes
		//Start New Code  Date: 03/23/2014
		logger.info("My id from Server.java : "+myId);		
		//End New Code  Date: 03/23/2014
		// create manager for accepting jobs
		jobMgr = JobManager.getInstance(myId); // May need to update jobmanager - 
		// establish nearest nodes and start receiving heartbeats
		heartbeatMgr = HeartbeatManager.getInstance(myId);
		for (NodeDesc nn : conf.getNearest().getNearestNodes().values()) {
			HeartbeatData node = new HeartbeatData(nn.getNodeId(), nn.getHost(), nn.getPort(), nn.getMgmtPort());
			logger.info("Heartbeat getPort "+nn.getPort());
			HeartbeatConnector.getInstance().addConnectToThisNode(node);
		}
		heartbeatMgr.start();

		// manage heartbeatMgr connections
		HeartbeatConnector conn = HeartbeatConnector.getInstance();
		logger.info("Starting HB connections! ");
		conn.start();

		logger.info("Server " + myId + ", managers initialized");
	}

	/**
	 * 
	 */
	public void run() {
		if (conf == null) {
			logger.error("Missing configuration file");
			return;
		}

		String myId = conf.getServer().getProperty("node.id");
		logger.info("Initializing server " + myId);

		// storage initialization
		// TODO storage setup (e.g./ connection to a database)

		startManagers(); // Here it starts network, election and heartbeat manager

		StartManagement mgt = new StartManagement(conf);
		Thread mthread = new Thread(mgt);
		mthread.start();

		StartCommunication comm = new StartCommunication(conf);
		logger.info("Server " + myId + " ready");

		Thread cthread = new Thread(comm);
		cthread.start();

		MoocCommunication mooc = new MoocCommunication(conf);
		logger.info("Mooc " + myId + " ready");

		Thread moocthread = new Thread(mooc);
		//moocthread.start();

		/*boolean flag  = true;
		while(flag){
			logger.info("--in startManagers--Server.java--> Sending the first Election Message 2");
		electionMgr.sendElectionMsg(myId, VoteAction.ELECTION);
		flag = false;
		}*/

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
			System.err.println("Usage: java " + Server.class.getClass().getName() + " conf-file");
			System.exit(1);
		}

		File cfg = new File(args[0]);
		if (!cfg.exists()) {
			Server.logger.error("configuration file does not exist: " + cfg);
			System.exit(2);		}

		Server svr = new Server(cfg);
		svr.run();


	}

}
