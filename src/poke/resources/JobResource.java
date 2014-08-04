/*
 * copyright 2012, gash
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
package poke.resources;
import io.netty.channel.Channel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.Server;
import poke.server.management.managers.ElectionManager;
import poke.server.management.managers.JobManager;
import poke.server.management.managers.MoocChannel;
import poke.server.resources.Resource;
import poke.server.storage.jdbc.MyDBConnection;
import poke.server.storage.jdbc.SqlQueries;
import poke.server.storage.jdbc.TaskDBOperation;
import poke.server.resources.ResourceUtil;
import eye.Comm.Header;
import eye.Comm.JobDesc;
import eye.Comm.JobDesc.JobCode;
import eye.Comm.JobProposal;
import eye.Comm.JobStatus;
import eye.Comm.NameValueSet;
import eye.Comm.Payload;
import eye.Comm.Ping;
import eye.Comm.PokeStatus;
import eye.Comm.Request;
import eye.Comm.NameValueSet.NodeType;

public class JobResource implements Resource {
	protected static Logger logger = LoggerFactory.getLogger("server");
	
	public static HashMap<String, JobProposal> hm_req = new HashMap<String, JobProposal>(); //Contains request received to keep track  
	TaskDBOperation tsk = new TaskDBOperation();


	@Override
	public Request process(Request request) {

		JobManager jMgr = JobManager.getInstance();
		
		if(request.getBody().getJobOp().getData().getNameSpace().equalsIgnoreCase("competition")){
			jMgr.broadcast(request);
			//broadcast to other leaders
		} 
		else if (jMgr!=null){
			jMgr.sendJobProposal(createJobProposal(request));
		}else
			logger.info("Could not create job manager instance");
		Request.Builder rb = Request.newBuilder();

		// metadata
		rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), PokeStatus.SUCCESS, null));

		// payload
		Payload.Builder pb = Payload.newBuilder();
		JobStatus.Builder js = JobStatus.newBuilder();
		js.setJobId(request.getBody().getJobOp().getJobId());
		js.setStatus(PokeStatus.SUCCESS);
		js.setJobState(JobCode.JOBRECEIVED);
		pb.setJobStatus(js.build());
		rb.setBody(pb.build());

		Request reply = rb.build();
		logger.info("inside the job resource: response generated is:"+reply.toString());
		return reply;


	}
	/**
	 * The method creates JobProposal message to be sent internally to the nodes in order to get the bidding
	 * * @param req
	 * @return JobProposal
	 */
	public JobProposal createJobProposal(Request req){

		JobProposal.Builder jobProp = JobProposal.newBuilder();
		//these variables are set based on the values in the db based on the namespace value received from the client 
		int j_weight = 0;
		String job_id=null;
		NameValueSet.Builder ns = NameValueSet.newBuilder();
		NameValueSet.Builder ns1 = NameValueSet.newBuilder();
		List<NameValueSet> nlist = req.getBody().getJobOp().getData().getOptions().getNodeList();
		//	logger.info("<--Inside PerChannelQueue: createJobProposal() : size of nlist-->"+nlist.size());
		TaskDBOperation tdb=new TaskDBOperation();
		String namespace=req.getBody().getJobOp().getData().getNameSpace();
		if (req != null){
		
	/*	if(namespace.equals("competition")){
			jobProp.setOwnerId(req.getBody().getJobOp().getData().getOwnerId());
			j_weight=6;
			job_id="8";
		}
		else{*/			
			try{
				j_weight = tdb.getJobWeight(namespace);
				job_id=tdb.getJobId(namespace);
				jobProp.setOwnerId(Long.parseLong(ElectionManager.leaderNodeId));
				//Start New Code Date: 04/12/2014
				if(job_id!= null){
					Connection con = MyDBConnection.getConnection();
					logger.info("<--Job Resource: create Job proposal going to insert taskStatus: -->");
					try {
						PreparedStatement ps = con.prepareStatement(SqlQueries.str_insertTaskStatus);
						ps.setString(1,job_id );
						ps.setString(2,"RUNNING");
						int rs= ps.executeUpdate();
						if(rs<0){
							logger.info("Exception in Inserting the task status value in DB");
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						logger.info("Inside create Job proposal getting Exception :"+e);
						e.printStackTrace();
					}
				}
				//End New Code Date: 04/12/2014

			}catch(NullPointerException e){
				logger.error("unable to connect ");
			}
		}
			logger.info("jobid and jobweight received are :"+j_weight+job_id);
			
			
			
			jobProp.setNameSpace(namespace);
			jobProp.setJobId(job_id);
			jobProp.setWeight(j_weight);
			if(!nlist.isEmpty()){
				ns.setNodeType(NodeType.NODE);
				logger.info("size of nodelist is : "+nlist.size());
				for(int i= 0 ; i< nlist.size(); i++){
					ns1.setNodeType(NodeType.VALUE).setName(nlist.get(i).getName()).setValue(nlist.get(i).getValue());
					ns.addNode(i,ns1.build() ); //Left to be done  
				}
				//	req.get
				jobProp.setOptions(ns.build());
			}else {
				logger.info("nlist is null in Job Resourse send job prop mesg");
			}
			hm_req.put(job_id, jobProp.build());
		//}
		return jobProp.build();
	}



	public static JobProposal performTask(JobProposal req){
		String jobId = null;
		//boolean result = false;
		JobProposal jp=null;
		if(req != null){
			TaskDBOperation tdb=new TaskDBOperation();
			if(req.hasJobId()){
				jobId = req.getJobId();
				NameValueSet ns = null;
				logger.info("The jobid in job resource is ::"+jobId);
				if(jobId.equals("1")){
					ns=tdb.signUp(req);

				}else if(jobId.equals("2")){
					tdb.login(req);
					logger.info("received login job" + req);
					ns=tdb.login(req);
					jp=buildResponse(ns,req);
				}else if(jobId.equals("3")){
					ns=tdb.listCourses();
					//jp=buildResponse(ns,req);
					logger.info("received view courses job");
				}else if(jobId.equals("4")){
					ns=tdb.Enroll(req);
					//jp=buildResponse(ns,req);
					logger.info("received enroll to a course job");
				}else if(jobId.equals("5")){
					ns=tdb.viewCourseDescription(req);
					//jp=buildResponse(ns,req);
					logger.info("received course description to a course job");					
				}else{
					ns=tdb.viewEnrolledCourses(req);
					//jp=buildResponse(ns,req);
					logger.info("received list enroll courses to a course job");
				}
				if(ns!=null){
					jp=buildResponse(ns,req);
					
					
					
				}
			}

		}

		return jp;
	}
	public static JobProposal buildResponse(NameValueSet ns,JobProposal req){
		JobProposal.Builder rb = JobProposal.newBuilder();
		rb.setJobId(req.getJobId());
		rb.setNameSpace(req.getNameSpace());
		rb.setWeight(req.getWeight());
		rb.setOwnerId(req.getOwnerId());
		rb.setOptions(ns);
		//Start New Code Date: 04/12/2014
		if(req.getJobId()!= null){
			Connection con = MyDBConnection.getConnection();
			try {
				PreparedStatement ps = con.prepareStatement(SqlQueries.str_updateTaskStatus);
				ps.setString(1,String.valueOf(req.getOwnerId()));
				ps.setString(2,"DONE");
				ps.setString(3,req.getJobId());
				int rs= ps.executeUpdate();
				if(rs<0){
					logger.info("Exception in updating the task status value in DB");
				}else
					logger.info("<--Inside Job Manager: build response task-->updated the task status in DB =  "+rs);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.info("Inside create Job proposal getting Exception :"+e);
				e.printStackTrace();
			}
		}
		//End New Code Date: 04/12/2014
		return rb.build();
	}

}
