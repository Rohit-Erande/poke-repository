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
package poke.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.client.comm.CommConnection;
import poke.client.comm.CommListener;
import eye.Comm.Header;
import eye.Comm.JobDesc;
import eye.Comm.JobDesc.JobCode;

import eye.Comm.JobOperation;
import eye.Comm.JobOperation.JobAction;
import eye.Comm.JobProposal;
import eye.Comm.JobStatus;
import eye.Comm.NameSpace;
import eye.Comm.NameValueSet;
import eye.Comm.NameValueSet.NodeType;

import eye.Comm.Payload;
import eye.Comm.Ping;
import eye.Comm.PokeStatus;
import eye.Comm.Request;

/**
 * The command class is the concrete implementation of the functionality of our
 * network. One can view this as a interface or facade that has a one-to-one
 * implementation of the application to the underlining communication.
 * 
 * IN OTHER WORDS (pay attention): One method per functional behavior!
 * 
 * @author gash
 * 
 */
public class ClientCommand {
	protected static Logger logger = LoggerFactory.getLogger("client");

	private String host;
	private int port;
	private CommConnection comm;

	public ClientCommand(String host, int port) {
		this.host = host;
		this.port = port;

		init();
	}

	private void init() {
		comm = new CommConnection(host, port);
	}

	/**
	 * add an application-level listener to receive messages from the server (as
	 * in replies to requests).
	 * 
	 * @param listener
	 */
	public void addListener(CommListener listener) {
		comm.addListener(listener);
	}

	/**
	 * Our network's equivalent to ping
	 * 
	 * @param tag
	 * @param num
	 */
	public void poke(String tag, int num) {
		// data to send
		Ping.Builder f = eye.Comm.Ping.newBuilder();
		f.setTag(tag);
		f.setNumber(num);
		//Start new code 
		eye.Comm.JobOperation.Builder j=JobOperation.newBuilder();
		j.setJobId("1");
		j.setAction(JobAction.ADDJOB);
		//Start new code 
		// payload containing data
		Request.Builder r = Request.newBuilder();
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setPing(f.build());
		//Start new code 
		p.setJobOp(j);
		//Start new code 
		r.setBody(p.build());

		// header with routing info
		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setTag("test finger");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(eye.Comm.Header.Routing.PING);
		r.setHeader(h.build());

		eye.Comm.Request req = r.build();

		try {
			comm.sendMessage(req);
		} catch (Exception e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}


	/**
	 * @param tag
	 * @param num
	 */
	public void sendLogin(String username, String password){
		
		Request.Builder r = Request.newBuilder();
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		// header with routing info
		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setTag("Testing..");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(eye.Comm.Header.Routing.JOBS);
		r.setHeader(h.build());
		
		NameValueSet.Builder ns = NameValueSet.newBuilder();
	
		NameValueSet.Builder ns1 = NameValueSet.newBuilder();
		ns.setNodeType(NodeType.NODE);	
		ns1.setNodeType(NodeType.VALUE);
		ns1.setName("Username");
		ns1.setValue(username);
		ns.addNode(0, ns1.build());
		ns1.setNodeType(NodeType.VALUE);
		ns1.setName("Password");
		ns1.setValue(password);
		ns.addNode(1, ns1.build());
		
		
				
		JobDesc.Builder data = JobDesc.newBuilder();
		data.setOwnerId(port);
		data.setJobId("2");
		data.setStatus(JobCode.JOBRECEIVED);
		data.setNameSpace("login");// NameSpace will contain Job Description
		data.setOptions(ns.build());

		JobOperation.Builder jbOp = JobOperation.newBuilder();
		jbOp.setAction(JobAction.ADDJOB);
		jbOp.setJobId("2");
		jbOp.setData(data.build());
		
		p.setJobOp(jbOp.build());
		r.setBody(p.build());
		
		eye.Comm.Request req = r.build();
		logger.info("request generated for job desc on client side : "+req);
		try {
			comm.sendMessage(req);
		} catch (Exception e) {
			logger.warn("Unable to deliver message, queuing");
		}
		//JobDesc data to be sent with respect to task to be performed.

		/*JobStatus.Builder job = JobStatus.newBuilder();
		job.setJobId("2");
		job.setStatus(PokeStatus.SUCCESS);
		job.setJobState(JobCode.JOBRECEIVED);
		job.setData(1, data.build());

		job.build();*/
	}
	
	
	//End New Code -Date: 03/28/2014
}
