package edu.jlime.rpc.fr;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.util.ByteBuffer;

class NACKCounter {

	final boolean RANGE = true;
	final boolean SINGLE = false;

	private Logger log = Logger.getLogger(NACKCounter.class);

	Address to;

	ConfirmData[] rcvd;

	private volatile int nextExpectedNumber = 0;

	private ResendData[] resendArray;

	private volatile AtomicInteger seqN = new AtomicInteger(0);

	private int max_resend_size;

	private volatile AtomicInteger confirmed = new AtomicInteger(-1);

	private volatile AtomicInteger ackSenderCursor = new AtomicInteger(0);

	private NACK ack;

	Message ackMsg;

	public NACKCounter(NACK ack, Address to, int max_nack_size, int nack_delay,
			int ack_delay, Configuration config) {
		this.ack = ack;
		this.max_resend_size = config.ack_max_resend_size;
		resendArray = new ResendData[max_resend_size];
		for (int i = 0; i < resendArray.length; i++) {
			resendArray[i] = new ResendData();
		}
		this.to = to;
		rcvd = new ConfirmData[max_resend_size];
		for (int i = 0; i < rcvd.length; i++) {
			rcvd[i] = new ConfirmData();
		}
		this.ackMsg = Message.newEmptyOutDataMessage(MessageType.ACK, to);

	}

	public void send(Message msg) throws Exception {
		int seqN = this.seqN.getAndIncrement();

		while (Math.abs(seqN - confirmed.get()) >= max_resend_size) {
			log.info("Blocking on seq " + seqN);
			synchronized (this.seqN) {
				this.seqN.wait(0, 500);
			}
		}

		// log.info("Sending seq " + seqN);

		resendArray[pos(seqN)].setData(msg, System.currentTimeMillis(), seqN);

		Message ackSeqMsg = Message
				.encapsulateOut(msg, MessageType.ACK_SEQ, to);
		ackSeqMsg.getHeaderBuffer().putInt(seqN);
		ackSeqMsg.getHeaderBuffer().putInt(nextExpectedNumber);
		ack.sendNext(ackSeqMsg);
	}

	public synchronized boolean seqNumberArrived(int seq) throws Exception {
		if (seq < nextExpectedNumber)
			return false;

		if (seq % (rcvd.length / 2) == 0)
			sendNextExpectedNumber();

		rcvd[pos(seq)].confirmed = true;
		rcvd[pos(seq)].seq = seq;

		if (seq == nextExpectedNumber) {
			while (rcvd[pos(nextExpectedNumber)].confirmed) {
				rcvd[pos(nextExpectedNumber)].confirmed = false;
				rcvd[pos(nextExpectedNumber)].seq = -1;
				nextExpectedNumber++;
			}
		}

		return true;

	}

	private int pos(int seqN) {
		return seqN % max_resend_size;
	}

	public void confirm(int seq) {
		if (seq <= confirmed.get())
			return;

		ResendData curr = resendArray[pos(seq)];
		curr.setConfirmed();
		if (seq == confirmed.get() + 1) {
			synchronized (confirmed) {
				if (seq == confirmed.get() + 1) {

					resendArray[pos(confirmed.get() + 1)].data = null;

					confirmed.incrementAndGet();

					boolean done = false;
					while (!done) {
						ResendData res = resendArray[pos(confirmed.get() + 1)];
						if (res.data != null && res.isConfirmed()) {
							res.data = null;
							confirmed.incrementAndGet();
						} else
							done = true;
					}
				}
			}
		}
	}

	public boolean sendAcks() throws Exception {
		ackMsg.getHeader().clear();

		int currSize = ackMsg.getSize();

		appendNack(ackMsg, 32, rcvd.length);

		if (ackMsg.getSize() == currSize)
			return false;
		this.ack.sendNext(ackMsg);
		return true;
	}

	public void sendNextExpectedNumber() throws Exception {
		Message msg = Message.newEmptyOutDataMessage(MessageType.SYN, to);
		msg.getHeaderBuffer().putInt(nextExpectedNumber);
		ack.sendNext(msg);
	}

	private void appendNack(Message msg, int max_obtained, int max_total) {
		ByteBuffer buff = msg.getHeaderBuffer();

		int diff = ack.max_size - msg.getSize();
		if (diff > 4 + 1 + 4 + 4) {

			buff.putInt(nextExpectedNumber);

			int appended = 0;

			int from = -1;
			int to = -1;

			int count = 0;
			int gathered = 0;

			while (appended < diff && gathered < max_obtained
					&& count < max_total && count < rcvd.length) {
				int i = ackSenderCursor.getAndIncrement() % rcvd.length;
				count++;
				if (!rcvd[i].confirmed && rcvd[i].seq != -1) {
					int seq = rcvd[i].seq;
					gathered++;
					if (from == -1) {
						from = seq;
						to = seq;
					} else if (to != seq - 1) {
						if (to != from) {
							buff.putBoolean(RANGE);
							buff.putInt(from);
							buff.putInt(to);
							appended += 1 + 4 + 4;
						} else {
							buff.putBoolean(SINGLE);
							buff.putInt(from);
							appended += 1 + 4;
						}
						from = seq;
						to = seq;
					} else
						to++;
				}
			}
			// }
			if (from != -1) {
				if (to != from) {
					buff.putBoolean(RANGE);
					buff.putInt(from);
					buff.putInt(to);
				} else {
					buff.putBoolean(SINGLE);
					buff.putInt(from);
				}
			}
		}
	}

	public void receivedAckBuffer(ByteBuffer buffer) throws Exception {
		if (buffer.hasRemaining())
			sync(buffer.getInt());
		while (buffer.hasRemaining()) {
			boolean type = buffer.getBoolean();
			if (type == RANGE) {
				int from = buffer.getInt();
				int to = buffer.getInt();

				log.info("Received NACK range :" + from + "-" + to);
				for (int i = from; i <= to; i++) {
					resend(i);
				}
			} else {

				int from = buffer.getInt();
				log.info("Received NACK :" + from);
				resend(from);
			}
		}

	}

	private void resend(int seq) throws Exception {
		if (seq < confirmed.get())
			return;
		Message data = resendArray[pos(seq)].data;
		if (data != null) {
			Message ackMsg = Message.encapsulateOut(data, MessageType.ACK_SEQ,
					to);
			ackMsg.getHeaderBuffer().putInt(seq);
			ack.sendNext(ackMsg);
		}
	}

	public void sync(int remoteNextExpected) {
		for (int i = confirmed.get(); i < remoteNextExpected; i++)
			confirm(i);
	}
}
