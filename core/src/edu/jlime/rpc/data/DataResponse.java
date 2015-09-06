package edu.jlime.rpc.data;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.message.Message;

public class DataResponse {

	private DataProcessor dp;
	// private Semaphore wait = new Semaphore(0);
	private volatile Message msg = null;
	private int id;
	private Address addr;

	public DataResponse(DataProcessor dataProcessor, Address addr, int msgID) {
		this.dp = dataProcessor;
		this.id = msgID;
		this.addr = addr;
	}

	public Message getResponse() {
		// try {
		// wait.acquire();
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		int cont = 0;
		while (msg == null) {
			cont++;
			if (cont == 1000) {
				synchronized (this) {
					try {
						wait(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				cont = 0;
			}
		}
		dp.removeResponse(addr, id);
		return msg;
	}

	public synchronized void setMsg(Message msg) {
		if (this.msg == null) {
			this.msg = msg;
			notify();
		}
	}

}
