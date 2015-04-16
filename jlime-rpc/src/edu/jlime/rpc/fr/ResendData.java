package edu.jlime.rpc.fr;

import edu.jlime.rpc.message.Message;

public class ResendData {

	volatile Message data = null;

	volatile long timeSent = -1;

	volatile int seq = -1;

	volatile boolean confirmed = false;

	public void setConfirmed() {
		this.confirmed = true;
	}

	public boolean isConfirmed() {
		return confirmed;
	}

	synchronized void setData(Message data, long timeSent, int seq) {
		this.confirmed = false;
		this.timeSent = timeSent;
		this.seq = seq;
		this.data = data;
	}

	synchronized public Message getData(int seq2) {
		if (seq2 == seq)
			return data;
		return null;
	}
}