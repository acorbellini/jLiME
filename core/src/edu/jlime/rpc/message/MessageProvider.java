package edu.jlime.rpc.message;

public interface MessageProvider {

	public void addMessageListener(MessageType type, MessageListener packList);
}
