package poke.server.management.managers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.Management;


public class MoocPrintListener implements MoocListener 
{
	protected static Logger logger = LoggerFactory.getLogger("mooc");
	
	private String nodeID;
	
	public MoocPrintListener(String nodeID)
	{
		this.nodeID = nodeID;
	}
	
	
	@Override
	public String getListenerID() {
		// TODO Auto-generated method stub
		return nodeID;
	}

	@Override
	public void onMessage(Management msg) {
		// TODO Auto-generated method stub
		if (logger.isDebugEnabled())
			logger.debug(msg.getJobPropose().getJobId());
		
		// if the nodeID is set, we filter messages on it
		
		if (msg.hasJobPropose()) {
			logger.info("Received graph responses from " + msg.getJobPropose().getOwnerId());
		} else
			logger.error("Received management response from unexpected host: " + msg.getJobPropose().getOwnerId());
	}

	@Override
	public void connectionClosed() {
		// TODO Auto-generated method stub
		logger.error("Management port connection for internMooc communication failed");
	}

	@Override
	public void connectionReady() {
		// TODO Auto-generated method stub
		logger.info("Management port for interMooc communication is ready to receive messages");
	}

}
