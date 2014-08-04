/*
 * copyright 2013, gash
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.LeaderElection.VoteAction;
import poke.monitor.MonitorListener;
import poke.server.Server;

public class HeartbeatListener implements MonitorListener {
	protected static Logger logger = LoggerFactory.getLogger("management");

	private HeartbeatData data;
	
	public static boolean gotFirstElectMesg = false;
	public static String nextNodeId=  null;
	
	public HeartbeatListener(HeartbeatData data) {
		this.data = data;
	}

	public HeartbeatData getData() {
		return data;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.monitor.MonitorListener#getListenerID()
	 */
	@Override
	public String getListenerID() {
		return data.getNodeId();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.monitor.MonitorListener#onMessage(eye.Comm.Management)
	 */
	@Override
	public void onMessage(eye.Comm.Management msg) {
		if (logger.isDebugEnabled())
			logger.debug(msg.getBeat().getNodeId());

		if (msg.hasGraph()) {
			logger.info("Received graph responses");
		} else if (msg.hasBeat() && msg.getBeat().getNodeId().equals(data.getNodeId())) {
			logger.info("Received HB response from " + msg.getBeat().getNodeId());
			
			nextNodeId = msg.getBeat().getNodeId();
						
			data.setLastBeat(System.currentTimeMillis());
		} //Start New Code to handle the message received for Election - 
		else if(msg.hasElection()){ 
			logger.info("<--In Heartbeat Listener hasElection-->Got the election Message --> desc: "+msg.getElection().getDesc());
			if (msg.getElection().getVote().equals(VoteAction.ELECTION)){
				gotFirstElectMesg = true;
			}
			ElectionManager.getInstance().processRequest(msg.getElection());
		}else if (msg.hasJobPropose()){
			logger.info("<--In Heartbeat Listener hasElection-->Got the Job proposal Message --> Job Id: "+msg.getJobPropose().getJobId());
			/*if(msg.getJobPropose().getOwnerId() == Long.parseLong(Server.myId)){
				// Means the jobPropose message has been circulated in the cluster and has returned back to the Leader.
				//Perform other operation . To be done later.
				logger.info("todo perform assign job!");
			}else{	*/			
				JobManager.getInstance().processRequest(msg.getJobPropose());
				/*if(msg.getJobPropose().getNameSpace().equalsIgnoreCase(Server.myId)){
					//Logic to send first bidding message by the node
					
				}				
			}*/	
		}else if (msg.hasJobBid()){
			logger.info("<--In Heartbeat Listener hasJob bid-->Got the Job bid Message --> Job Id: "+msg.getJobBid().getJobId());
			JobManager.getInstance().processRequest(msg.getJobBid());
		}
		
		else
			logger.error("Received heartbeatMgr from on wrong channel or unknown host: " + msg.getBeat().getNodeId());
	}

	@Override
	public void connectionClosed() {
		// note a closed management port is likely to indicate the primary port
		// has failed as well
	}

	@Override
	public void connectionReady() {
		// do nothing at the moment
	}
}
