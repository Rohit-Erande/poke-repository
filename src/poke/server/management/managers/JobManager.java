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

import io.netty.channel.Channel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.client.comm.CommConnection;
import poke.client.comm.CommHandler;
import poke.monitor.MonitorListener;
import poke.resources.JobResource;

import poke.server.Server;
import poke.server.management.ManagementQueue;
import poke.server.queue.ChannelQueue;
import poke.server.queue.PerChannelQueue;
import poke.server.queue.QueueFactory;
import poke.server.resources.Resource;
import poke.server.resources.ResourceFactory;
import poke.server.resources.ResourceUtil;
import poke.server.storage.jdbc.MyDBConnection;
import poke.server.storage.jdbc.SqlQueries;
import poke.server.storage.jdbc.jobMapper;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;

import eye.Comm.Heartbeat;
import eye.Comm.JobBid;
import eye.Comm.JobBid.Builder;
import eye.Comm.JobDesc.JobCode;
import eye.Comm.JobOperation.JobAction;
import eye.Comm.NameSpaceOperation.SpaceAction;
import eye.Comm.Header;
import eye.Comm.JobDesc;
import eye.Comm.JobOperation;
import eye.Comm.JobProposal;
import eye.Comm.JobStatus;
import eye.Comm.Management;
import eye.Comm.NameSpace;
import eye.Comm.NameSpaceOperation;
import eye.Comm.NameSpaceStatus;
import eye.Comm.NameValueSet;
import eye.Comm.Payload;
import eye.Comm.Ping;
import eye.Comm.PokeStatus;
import eye.Comm.Request;

/**
 * The job manager class is used by the system to assess and vote on a job. This
 * is used to ensure leveling of the servers take into account the diversity of
 * the network.
 * 
 * @author gash
 * 
 */
public class JobManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<JobManager> instance = new AtomicReference<JobManager>();
	protected PerChannelQueue pq;

	public static HashMap<String,Request> hm = new HashMap<String, Request>(); // Use to keep track of response to be send to the client back
	public static HashMap<String,String> hash_jobid = new HashMap<String, String>();
	public static HashMap<String,JobProposal> hm1 = new HashMap<String, JobProposal>();
	//End code for hashmap for keeping the track of responses to be send to the client 
	
	private static int count=0;
	private static Long count_forLeader=0L;
	private static Long node_Id=0L;
	private static int job_Bid=0;
	private static String job_Id;
	Management msg;
	
	private String nodeId;
	
	String myId;
	private String id;
	JobBid.Builder jbid = JobBid.newBuilder();
	

	public static JobManager getInstance(String id) {
		instance.compareAndSet(null, new JobManager(id));
		return instance.get();
	}

	public static JobManager getInstance() {
		return instance.get();
	}

	public JobManager(String nodeId) {
		this.nodeId = nodeId;
	}

	/**
	 * a new job proposal has been sent out that I need to evaluate if I can run
	 * it
	 * 
	 * @param req
	 *            The proposal
	 */
	public void processRequest(JobProposal req) {
		
		//	JobBid.Builder jBid = JobBid.newBuilder();
		myId = Server.myId;
		logger.info("inside the job manager process request method req got = "+req);
		
		if(req.hasJobId()){
			if(req!=null && !myId.equalsIgnoreCase(ElectionManager.leaderNodeId)){
				logger.info("Namespace received is"+req.getNameSpace());
				if(req.getOwnerId() == Long.parseLong(ElectionManager.leaderNodeId) || req.getNameSpace().equalsIgnoreCase("competition_internal")){
					sendJobProposal(req);
					logger.info("Job proposal message forwarded, now sending jobbid message !");
					//if(req.getNameSpace().equals(myId))
					createJobBidMesg(req);
					//	sendJobBid();
					logger.info("Job bidding message sent!");
				}else if (req.getOwnerId() == Long.parseLong(Server.myId)){
					//Means the job propsal has been send by leader to me for performing the assigned task
					//performAssignJob(req);
					logger.info("todo perform assign job!");
					sendJobProposal(JobResource.performTask(req));//node sends the response back to the leader

				}else{
					//The message is send by the leader to the specified node to perform the assigned task and I need to forward it to the appropriate node
					sendJobProposal(req);
				}
			}else{

				logger.info("Leader got the jobProposal message back waiting for Job bid to assign job!");
				if(req.getOwnerId()!=Long.parseLong(Server.myId)){
					
					logger.info("the owner id is ::"+req.getOwnerId()+"The count for leader is :: "+ count_forLeader+"and the namespace is::"+req.getNameSpace());

					logger.info("Leader got a namespace:::------------ "+req.getNameSpace());
					if(req.getNameSpace().equalsIgnoreCase("competition")&& req.getOwnerId()!=count_forLeader){
						count_forLeader=req.getOwnerId();
						logger.info("Leader got a competition job proposal");
						//forward the competition job proposal
						JobProposal.Builder jobProp1 = JobProposal.newBuilder();
						jobProp1.setNameSpace("competition_internal");
						jobProp1.setOwnerId(req.getOwnerId());
						jobProp1.setJobId(req.getJobId());
						jobProp1.setWeight(req.getWeight());
						sendJobProposal(jobProp1.build());

					}
					else if(!req.getNameSpace().equalsIgnoreCase("competition_internal")){
						logger.info("leader got a response back from node");
						//leader creates a  a job status and sends it back to client
						Request.Builder rb = Request.newBuilder();
						Payload.Builder pb = Payload.newBuilder();
						JobStatus.Builder js = JobStatus.newBuilder();
						js.setJobId(req.getJobId());
						js.setStatus(PokeStatus.SUCCESS);
						js.setJobState(JobCode.JOBRECEIVED);
						JobDesc.Builder jd=JobDesc.newBuilder();
						jd.setNameSpace(req.getNameSpace());
						jd.setOwnerId(req.getOwnerId());
						jd.setStatus(JobCode.JOBRUNNING);
						jd.setJobId(req.getJobId());
						jd.setOptions(req.getOptions());
						js.addData(0, jd.build());
						pb.setJobStatus(js.build());
						rb.setBody(pb.build());
						eye.Comm.Header.Builder h = Header.newBuilder();
						h.setOriginator("server");
						h.setTag("test finger");
						h.setTime(System.currentTimeMillis());
						h.setRoutingId(eye.Comm.Header.Routing.JOBS);
						rb.setHeader(h.build());

						Request reply = rb.build();
						logger.info("Reply here req.getJobId(): -----------"+req.getJobId());
						logger.info("Reply here reply: -----------"+reply);

						hm.put(req.getJobId(), reply);
						logger.info("hm size after putting mesSAGE :"+hm.size());
					}
				}
				
			}
			
		}

	}
	
	private void taskAssignment(JobBid jbid) {
		// TODO Auto-generated method stub
		logger.info("<--Inside Job Manager--> Assign Job ---:");
		JobProposal jp = null;
		
		//leader received a jobBid message
		//leader will compare the previous bid with the one he received 
		if(job_Bid<=jbid.getBid()){
			count ++;
			job_Bid=jbid.getBid();
			node_Id=jbid.getOwnerId();
			job_Id=jbid.getJobId();
		}

		logger.info("count = "+count+" : Integer.parseInt(Server.myId) : "+Integer.parseInt(Server.myId));
		if(count==Integer.parseInt(Server.myId)-2){
			
			if(jbid.getNameSpace().equals("competition_internal")){
				JobBid.Builder jobBid =JobBid.newBuilder();
				if(job_Bid>5){
					hash_jobid.put(jbid.getJobId(),"1");
				}
				else
					hash_jobid.put(jbid.getJobId(),"0");
				jobBid.setNameSpace("competition");
				jobBid.setJobId(jbid.getJobId());
				jobBid.setOwnerId(jbid.getOwnerId());
				//jobBid.build();
				/*MoocChannel mc=new MoocChannel("192.168.0.50",5677);
			Channel ch=	mc.connect();
			if (ch == null || !ch.isOpen()) {
				logger.error("connection missing, no outbound communication");
			}
				if (ch.isWritable()) {
					logger.info("Channel is writable");
					MoocHandler handler = mc.connect().pipeline().get(MoocHandler.class);

				MoocListener mlistener=new MoocPrintListener("testing ");
				mc.addListener(mlistener);

				logger.info("channel created is"+ch);
				Management.Builder request = Management.newBuilder().setJobBid(jobBid.build());
				msg=request.build();
				//ch.writeAndFlush(msg);
				}*/

			}
			else if(jbid.getNameSpace().equalsIgnoreCase("competition")){
				logger.info("Owner id of the winner is "+jbid.getOwnerId());
			}
			else{
				logger.info("Leader is assigning task to node : "+nodeId);
				JobProposal.Builder jobProp = JobProposal.newBuilder();
				jobProp.setNameSpace(jbid.getNameSpace());
				jobProp.setOwnerId(node_Id);
				jobProp.setJobId(job_Id);
				jobProp.setWeight(job_Bid);
				NameValueSet ns = null;
				//Start New Code Date: 04/12/2014
				if(node_Id!= null){
					Connection con = MyDBConnection.getConnection();
					try {
						PreparedStatement ps = con.prepareStatement(SqlQueries.str_updateTaskStatus);
						ps.setString(1,node_Id.toString());
						ps.setString(2,"ASSIGNED");
						ps.setString(3,job_Id);
						int rs= ps.executeUpdate();
						if(rs<0){
							logger.info("Exception in updating the task status value in DB");
						}else
							logger.info("<--Inside Job Manager: assign task-->updated the task status in DB  ="+rs);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						logger.info("Inside create Job proposal getting Exception :"+e);
						e.printStackTrace();
					}
				}
				//End New Code Date: 04/12/2014


				//New Code to retrieve the name value set from job proposal earlier sent to be forwarded to the node
				hm1 = JobResource.hm_req;
				if(!hm1.isEmpty()){
					jp = hm1.get(job_Id);
					logger.info("Inside task assignment job proposal got from hashmap:  "+jp);
					if (jp.hasOptions()){
						ns = jp.getOptions();
						logger.info("Inside task assignment job proposal ns created:  "+ns);
						if (ns != null){
							logger.info("Coming here.!!!!.................");
							jobProp.setOptions(ns);
						}
					}
				}
				
				sendJobProposal(jobProp.build());
			}
			count=0;
			job_Bid=0;
		}
		
	}
	
	/**
	 * a job bid for my job
	 * 
	 * @param req
	 *            The bid
	 */
	public void processRequest(JobBid req) {

		if (Server.myId.equalsIgnoreCase(ElectionManager.leaderNodeId)){
			//Perform task assignment operation.

			logger.info(" Leader got the jobbid perform the task assignment operation!");
			taskAssignment(req);
		}else{
			sendJobBid(req);
		}


	}
	
	public void sendJobProposal(JobProposal req){
		try{
				

			
			logger.info("inside job proposal method");

			Management.Builder b = Management.newBuilder();
			b.setJobPropose(req);
			logger.info("job proposal sent");
			//	logger.info("channel is"+channel.toString());
			for(HeartbeatData hb : HeartbeatManager.getInstance().outgoingHB.values())
			{ 
				logger.info("<--Inside JobManager:sendJobProposal-->HB channel = "+hb.channel.localAddress().toString());
				if (hb.channel.isOpen()) {
					logger.info("<--Inside JobManager:sendJobProposal-->Channel is open! Value of Channel: "+hb.getChannel().toString());
					if (hb.channel.isWritable()){
						hb.channel.flush();
						logger.info("Message send successfully!");
						ManagementQueue.enqueueResponse( b.build(), hb.channel);
					}
				}
			}

		}catch(Exception e){
			logger.info(""+e);
		}	
	}

	public void createJobBidMesg(JobProposal req){
		jbid.setJobId(req.getJobId());
		jbid.setNameSpace(req.getNameSpace());
		jbid.setOwnerId(Long.parseLong(Server.myId));
		jbid.setBid(getJobBid());
		logger.info("Inside job manager : constructed bid message it is : "+jbid.build());
		//return jbid.build();
		sendJobBid(jbid.build());
	}

	private int getJobBid() {
		// TODO Auto-generated method stub
		//Get load from database table sum of all weights of jobs assigned to that node.
		int load = 5;
		int bidValue = ((10-load)<=0)? 0:(10-load);
		return bidValue;
	}

	private void sendJobBid(JobBid jobBidMesg) {
		// TODO Auto-generated method stub
		try{
			logger.info("inside job bid method");

			Management.Builder b = Management.newBuilder();
			b.setJobBid(jobBidMesg);
			logger.info("job bid sent");
			//	logger.info("channel is"+channel.toString());
			for(HeartbeatData hb : HeartbeatManager.getInstance().outgoingHB.values())
			{ 
				logger.info("<--Inside JobManager:sendJobBid-->HB channel= "+hb.channel.localAddress().toString());
				if (hb.channel.isOpen()) {
					logger.info("<--Inside JobManager:sendJobBid-->Channel is open! Value of Channel: "+hb.getChannel().toString());
					if (hb.channel.isWritable()){
						hb.channel.flush();
						logger.info("Job Bid Message send successfully!");
						ManagementQueue.enqueueResponse( b.build(), hb.channel);
					}
				}
			}
		}catch(Exception e){
			logger.info(""+e);
		}	
	}
	
	public void broadcast(Request request) {
		//	if(req.getNameSpace().equalsIgnoreCase("competition")){
		logger.info("--------------------------inside job proposal got a competition from client-----------------------");
		MoocChannel mc=new MoocChannel("192.168.0.2",5571);
		logger.info("--------------------------inside job Manager got a competition from client-before connect-------------");
		Channel ch=	mc.connect();
		if (ch == null || !ch.isOpen()) {
			logger.error("connection missing, no outbound communication");
		}else if (ch.isWritable()) {
			logger.info("Channel is writable");
			MoocHandler handler = mc.connect().pipeline().get(MoocHandler.class);

			MoocListener mlistener=new MoocPrintListener("broadcast");
			mc.addListener(mlistener);
			NameSpaceOperation.Builder ns=NameSpaceOperation.newBuilder();
			ns.setAction(SpaceAction.ADDSPACE);
			NameSpace.Builder ns1=NameSpace.newBuilder();
			ns1.setNsId(2L);
			ns1.setName(request.getBody().getSpaceOp().getData().getName());
			ns1.setDesc(request.getBody().getSpaceOp().getData().getDesc());
			ns1.setCreated(request.getBody().getSpaceOp().getData().getCreated());
			ns1.setOwner(request.getBody().getSpaceOp().getData().getOwner());
			ns.setData(ns1.build());
			Request.Builder rb = Request.newBuilder();
			Payload.Builder pb = Payload.newBuilder();
			pb.setSpaceOp(ns.build());
			rb.setBody(pb.build());
			rb.setHeader(request.getHeader());
			Request reply = rb.build();
			ch.writeAndFlush(reply);
			logger.info("channel created is"+ch.toString());
		}
		logger.info("---------------------------Listener added:: Inside Job Manager------------------------");
		//logger.info("req sending to other leader :"+req.toString());

	}

}








