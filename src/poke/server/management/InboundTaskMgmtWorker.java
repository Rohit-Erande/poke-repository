package poke.server.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.Management;
import poke.server.management.ManagementQueue.ManagementQueueEntry;

public class InboundTaskMgmtWorker extends Thread{


	protected static Logger logger = LoggerFactory.getLogger("management");

	int workerId;
	boolean forever = true;

	public InboundTaskMgmtWorker(ThreadGroup tgrp, int workerId) {
		super(tgrp, "inbound-task-mgmt-" + workerId);
		this.workerId = workerId;

		if (ManagementQueue.outbound == null)
			throw new RuntimeException("connection worker detected null queue");
	}

	@Override
	public void run() {
		while (true) {
			if (!forever && ManagementQueue.inbound.size() == 0)
				break;

			try {
				// block until a message is enqueued
				ManagementQueueEntry msg = ManagementQueue.inbound.take();

				if (logger.isDebugEnabled())
					logger.debug("Inbound management message received");

				logger.info("<--Inside InboundMgmt Queue-> Inside try msg :" + msg.req.getElection());


				Management req = (Management) msg.req;

				if (req.hasBeat()) {
					/**
					 * Incoming: this is from a node that this node requested to
					 * create a connection (edge) to. In other words, we need to
					 * track that this connection is healthy - get a
					 * heartbeatMgr.
					 * 
					 * Incoming are connections this node establishes, which is
					 * handled by the HeartbeatConnector.
					 */
					/*HeartbeatManager.getInstance().processRequest(req.getBeat());
					} else if (req.hasElection()) {
						//Got the ElectionMessage 
						logger.info("Got the election Message --> Forwarding it to processRequest"); 
						ElectionManager.getInstance().processRequest(req.getElection());
					} else if (req.hasGraph()) {
						NetworkManager.getInstance().processRequest(req.getGraph(), msg.channel, msg.sa);
					} else if (req.hasJobBid()) {
						JobManager.getInstance().processRequest(req.getJobBid());
					} else if (req.hasJobPropose()) {
						JobManager.getInstance().processRequest(req.getJobPropose());*/
				} else
					logger.error("Unknown management message");

				/*} catch (InterruptedException ie) {
					break;*/
			} catch (Exception e) {
				logger.error("Unexpected processing failure", e);
				break;
			}
		}

		if (!forever) {
			logger.info("connection queue closing");
		}

	}
}
