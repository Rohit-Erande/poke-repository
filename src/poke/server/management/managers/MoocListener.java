package poke.server.management.managers;

public interface MoocListener {

	public abstract String getListenerID();

	
	public abstract void onMessage(eye.Comm.Management msg);
	
	public abstract void connectionClosed();
	
	public abstract void connectionReady();

	
}
