package edu.jlime.rpc.data;

public abstract class Response {

	int msgID;

	public Response(int msgID) {
		this.msgID = msgID;
	}

	public abstract void sendResponse(byte[] resp) throws Exception;
}
