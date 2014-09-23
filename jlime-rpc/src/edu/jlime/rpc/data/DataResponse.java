package edu.jlime.rpc.data;

import java.util.UUID;
import java.util.concurrent.Semaphore;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.message.Message;

public class DataResponse {

	private DataProcessor dp;
	private Semaphore wait = new Semaphore(0);
	private volatile Message msg = null;
	private UUID id;
	private Address addr;

	public DataResponse(DataProcessor dataProcessor, Address addr, UUID msgID) {
		this.dp = dataProcessor;
		this.id = msgID;
		this.addr = addr;
	}

	public Message getResponse() {
		try {
			wait.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		dp.removeResponse(addr, id);
		return msg;
	}

	public void setMsg(Message msg) {
		this.msg = msg;
		wait.release();
	}

}
