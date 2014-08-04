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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.management.managers.JobManager;
import poke.server.resources.Resource;
import eye.Comm.JobProposal;
import eye.Comm.NameSpace;
import eye.Comm.NameSpaceStatus;
import eye.Comm.Payload;
import eye.Comm.PokeStatus;
import eye.Comm.Request;

public class NameSpaceResource implements Resource {
	protected static Logger logger = LoggerFactory.getLogger("server");

	JobManager jMgr = JobManager.getInstance();
	Payload.Builder pb = Payload.newBuilder();
	@Override
	public Request process(Request request) {
		//if(request.getBody().getSpaceOp().getData().getName().equalsIgnoreCase("competition")){
		int j_weight=6;
		long job_bid;
		Request reply = null;
		//count_forLeader=req.getOwnerId();
		logger.info("Leader got a competition job Namespace");
		//forward the competition job proposal

		if(!request.getHeader().getOriginator().equalsIgnoreCase("client")){

			JobProposal.Builder jobProp1 = JobProposal.newBuilder();
			jobProp1.setNameSpace("competition_internal");
			jobProp1.setOwnerId(Long.parseLong(request.getBody().getSpaceOp().getData().getOwner()));
			jobProp1.setJobId(String.valueOf((request.getBody().getSpaceOp().getData().getNsId())));
			jobProp1.setWeight(j_weight);
			jMgr.sendJobProposal(jobProp1.build());

			job_bid=Integer.parseInt(jMgr.hash_jobid.get((request.getBody().getSpaceOp().getData().getNsId())));
			logger.info("Client coming ----"+request.getBody().getSpaceOp().getData().getNsId());
			Request.Builder rb = Request.newBuilder();
			NameSpaceStatus.Builder ns1=NameSpaceStatus.newBuilder();
			ns1.setStatus(PokeStatus.SUCCESS);
			NameSpace.Builder ns=NameSpace.newBuilder();
			ns.setDesc(request.getBody().getSpaceOp().getData().getDesc());
			ns.setNsId(job_bid);
			ns1.setData(1, ns.build());
			pb.setSpaceStatus(ns1.build());
			rb.setBody(pb.build());

			reply = rb.build();
		}else{
			logger.info("Client coming broadcasting !!----"+request.getBody().getSpaceOp().getData().getNsId());
			jMgr.broadcast(request);
		}

		return reply;
	}
}
