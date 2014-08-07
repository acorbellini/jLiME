package edu.jlime.rpc.data;

import java.util.UUID;

public abstract class Response {

	UUID msgID;

	public Response(UUID msgID) {
		this.msgID = msgID;
	}

	public abstract void sendResponse(byte[] resp) throws Exception;
}
