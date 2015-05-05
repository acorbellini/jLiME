package edu.jlime.rpc.fr;

import edu.jlime.rpc.message.Message;

public class ResendData {

	volatile int timeout = -1;

	volatile Message data = null;

	volatile long timeSent = -1;

	volatile int seq = -1;

	volatile boolean confirmed = false;

	volatile boolean resend = false;

	public void setConfirmed() {
		this.confirmed = true;
	}

	public boolean isConfirmed() {
		return confirmed;
	}

	synchronized void setData(Message data, long timeSent, int seq, int timeout) {
		this.confirmed = false;
		this.timeSent = timeSent;
		this.seq = seq;
		this.data = data;
		this.timeout = timeout;
	}

	synchronized public Message getData(int seq2) {
		if (seq2 == seq)
			return data;
		return null;
	}
}