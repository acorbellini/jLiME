package edu.jlime.rpc.message;

public interface MessageListener {

	public abstract void rcv(Message msg, MessageProcessor origin) throws Exception;
}
