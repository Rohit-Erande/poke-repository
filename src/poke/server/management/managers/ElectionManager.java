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
package poke.server.management.managers;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;

import poke.monitor.HeartMonitor;
import poke.monitor.MonitorHandler;
import poke.monitor.MonitorInitializer;
import poke.monitor.MonitorListener;
import poke.monitor.HeartMonitor.MonitorClosedListener;
import poke.server.Server;
import poke.server.conf.ServerConf;
import poke.server.conf.ServerConf.GeneralConf;
import poke.server.management.ManagementQueue;
import eye.Comm.LeaderElection;
import eye.Comm.LeaderElection.VoteAction;
import eye.Comm.Management;

import poke.server.management.ManagementQueue.ManagementQueueEntry;

/**
 * The election manager is used to determine leadership within the network.
 * 
 * @author gash
 * 
 */
public class ElectionManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<ElectionManager> instance = new AtomicReference<ElectionManager>();

	protected ServerConf conf;
	protected String message;
	String myId;
	String desc;
	public static String leaderNodeId;
	Management msg;
	boolean hasLeader = false;

	//Atomic reference whose values are updated automatically. 
	private String nodeId;

	/** @brief the number of votes this server can cast */
	private int votes = 1;

	public static ElectionManager getInstance(String id, int votes) {
		instance.compareAndSet(null, new ElectionManager(id, votes));
		return instance.get();
	}

	public static ElectionManager getInstance() {
		return instance.get();
	}

	/**
	 * initialize the manager for this server
	 * 
	 * @param nodeId
	 *            The server's (this) ID
	 */
	protected ElectionManager(String nodeId, int votes) {
		this.nodeId = nodeId;

		if (votes >= 0)
			this.votes = votes;
	}
	/**
	 * @param sa 
	 * @param channel 
	 * @param args
	 */
	public void processRequest(LeaderElection req)  {

		if (req == null )
			return;
		
		ElectionManager electionMgr = ElectionManager.getInstance();
		myId  = Server.myId;
		logger.info("myId "+myId);
		
		if (req.hasExpires()) {
			long ct = System.currentTimeMillis();
			if (ct > req.getExpires()) {
				// election is over
				return;
			}
		}

		if (req.getVote().getNumber() == VoteAction.ELECTION_VALUE) {
			// an election is declared!
			//Start New code to handle the request for Election 
			if(myId.length() > 0 && !(req.getNodeId().equalsIgnoreCase(myId)) ){ //&& HeartbeatListener.gotFirstElectMesg == false
				//forward same id 
				logger.info("<--Inside ElectionManager: processReq-->Starting Election in Election_Value");
				desc = "Starting Election, message recieved from node"+myId;
				electionMgr.sendElectionMsg(req.getNodeId(), desc, VoteAction.ELECTION );	
				//HeartbeatListener.gotFirstElectMesg = true;
				//electionStarted = false;
			}
			if (req.getNodeId().equalsIgnoreCase(myId)){
				desc = "Sending NOMINATE_VALUE "+req.getNodeId()+" from node "+myId;
				electionMgr.sendElectionMsg(myId, desc, VoteAction.NOMINATE);
			}
			
		} else if (req.getVote().getNumber() == VoteAction.DECLAREVOID_VALUE) {
			// no one was elected, I am dropping into standby mode`
			
			logger.info("<--Inside ElectionManager: processReq-->VoteAction.DECLAREVOID_VALUE");
			
		} else if (req.getVote().getNumber() == VoteAction.DECLAREWINNER_VALUE) {
			// some node declared themself the leader
			
			leaderNodeId  = req.getNodeId();
			logger.info("<--Inside ElectionManager: processReq-->VoteAction.DECLAREWINNER_VALUE: "+leaderNodeId);
			if(!req.getNodeId().equalsIgnoreCase(myId)){
				leaderNodeId  = req.getNodeId();
				electionMgr.sendElectionMsg(req.getNodeId(), desc, VoteAction.DECLAREWINNER);
			}
			
		} else if (req.getVote().getNumber() == VoteAction.ABSTAIN_VALUE) {
			// for some reason, I decline to vote
			
			logger.info("<--Inside ElectionManager: processReq-->VoteAction.ABSTAIN_VALUE");
			
		} else if (req.getVote().getNumber() == VoteAction.NOMINATE_VALUE) {
			int comparedToMe = myId.compareTo(req.getNodeId());
			//if (comparedToMe < 0) {
			if (Integer.parseInt(req.getNodeId()) > Integer.parseInt(myId)) {
				// Someone else has a higher priority, forward nomination
				
				logger.info("<--Inside ElectionManager: processReq-->VoteAction.NOMINATE_VALUE: comparedToMe="+comparedToMe);
				if(!myId.isEmpty()){
					desc = "Sending NOMINATE_VALUE "+req.getNodeId()+" from node "+myId;
					electionMgr.sendElectionMsg(req.getNodeId(), desc, VoteAction.NOMINATE);
				}

			} else if (Integer.parseInt(req.getNodeId()) < Integer.parseInt(myId)) {
				// I have a higher priority, nominate myself
				// TODO nominate myself

				if(myId.length() > 0 ){
					logger.info("<--Inside ElectionManager: processReq-->VoteAction.NOMINATE_VALUE: comparedToMe="+comparedToMe);
					desc = "Sending NOMINATE_VALUE "+myId+" from node "+myId;
					electionMgr.sendElectionMsg(myId, desc, VoteAction.NOMINATE);
				}
			}else{
				logger.info("<--Inside ElectionManager: processReq-->VoteAction.NOMINATE_VALUE: comparedToMe="+comparedToMe);
				desc = "I'm the winner "+myId;
				if(!hasLeader){
					electionMgr.sendElectionMsg(req.getNodeId(), desc, VoteAction.DECLAREWINNER);
					hasLeader = true;
				}else{
					logger.info("<--Inside ElectionManager: processReq-->VoteAction.NOMINATE_VALUE: Everyone notified perform leader operations");
					//Start functionality of Leader.
				}
			}
		}

	}


	/**
	 * Function to generate Election message and send to the respective listening node.
	 * @param nodeId
	 * @param voteAction

	 */
	public void sendElectionMsg(String nodeId, String desc, VoteAction voteAction) {


		try {
			logger.info("<--Inside ElectionManager:sendElectionMsg-->sending election message");

			//msg = generateElectionMessage();
			if (nodeId.length()>0 && desc.length()>0 && voteAction != null){
				LeaderElection.Builder leaderElect = LeaderElection.newBuilder().setNodeId(nodeId).setBallotId("BallotId").setDesc(desc).setVote(voteAction);
				Management.Builder req = Management.newBuilder().setElection(leaderElect.build());
				msg= req.build();

				logger.info( "<--Inside ElectionManager:sendElectionMsg-->Size of outgoingHB : " +HeartbeatManager.getInstance().outgoingHB.size()+ "HeartbeatManager.getInstance().outgoingHB.values(): "+HeartbeatManager.getInstance().outgoingHB.values());
				for(HeartbeatData hb : HeartbeatManager.getInstance().outgoingHB.values())
				{
					logger.info("<--Inside ElectionManager:sendElectionMsg-->HB data!"+hb.channel.localAddress().toString());
					if (hb.channel.isOpen()) {
						logger.info("<--Inside ElectionManager:sendElectionMsg-->Channel is open! Value of Channel: "+hb.getChannel().toString());
						if (hb.channel.isWritable()){
							hb.channel.flush();
							//hb.channel.writeAndFlush(msg);
							logger.info("Message send successfully!");
							ManagementQueue.enqueueResponse(msg, hb.channel);
						}
					}else{
						logger.info("<--Inside ElectionManager:sendElectionMsg-->Channel not open!");
					}
					logger.info("<--Inside ElectionManager:sendElectionMsg-->leader election message sent");
				}
			}else{
				System.out.println("<--Inside ElectionManager:sendElectionMsg: NullPointerException-->Value of nodeId or desc or voteAction is null!");
			}
		} catch (Exception e) {
			logger.error("<--Inside ElectionManager:sendElectionMsg-->could not send connect to node", e);
		}
	}

}
