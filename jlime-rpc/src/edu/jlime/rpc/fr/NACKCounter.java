package edu.jlime.rpc.fr;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.util.ByteBuffer;

class NACKCounter {

	private static final int NACK_SIZE = 1 + 4 + 4;

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

	AtomicInteger sendNextExpectedCounter = new AtomicInteger(0);

	private volatile long maxSeqRcvd = -1l;

	private volatile boolean recheck = false;

	private int time;

	private float timeout_mult;

	public NACKCounter(NACK ack, Address to, Configuration config) {
		this.timeout_mult = config.timeout_mult;
		this.time = config.nack_resend_delay;
		this.ack = ack;
		this.max_resend_size = config.nack_max_resend_size;
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

		while (!ack.isStopped()
				&& Math.abs(seqN - confirmed.get()) >= max_resend_size) {
			if (log.isDebugEnabled())
				log.debug("Blocking on seq " + seqN);
			synchronized (this.seqN) {
				this.seqN.wait(0, 500);
			}
		}

		if (log.isDebugEnabled())
			log.debug("Sending seq " + seqN);

		resendArray[pos(seqN)].setData(msg, System.currentTimeMillis(), seqN,
				time);

		Message ackSeqMsg = Message
				.encapsulateOut(msg, MessageType.ACK_SEQ, to);
		ackSeqMsg.getHeaderBuffer().putInt(seqN);
		ackSeqMsg.getHeaderBuffer().putInt(nextExpectedNumber);

		// appendNack(ackSeqMsg);

		ack.sendNext(ackSeqMsg);
	}

	public synchronized boolean seqNumberArrived(int seq) throws Exception {
		if (seq < nextExpectedNumber || rcvd[pos(seq)].confirmed) {
			// if (log.isDebugEnabled())
			// log.debug("Ignoring seq " + seq + " next expected "
			// + nextExpectedNumber);
			// if (this.ack.metrics != null)
			// this.ack.metrics.counter("nack." + to + ".repeated").count();
			recheck = true;
			return false;
		}

		maxSeqRcvd = Math.max(maxSeqRcvd, seq);

		// if (this.ack.metrics != null)
		// this.ack.metrics.counter("nack." + to + ".rcvd").count();

		// if (log.isDebugEnabled())
		// log.debug("Sequence number arrived " + seq + " next expected "
		// + nextExpectedNumber);

		// if (seq % (rcvd.length / 2) == 0)
		// sendNextExpectedNumber();

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
		return Math.abs(seqN % max_resend_size);
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
					resendArray[pos(confirmed.get() + 1)].resend = false;

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

		if (log.isDebugEnabled())
			log.debug("Confirmed " + seq + " updated confirmed "
					+ confirmed.get());
	}

	public boolean sendNacks() throws Exception {
		ackMsg.getHeader().clear();

		int currSize = ackMsg.getSize();

		appendNack(ackMsg);

		if (ackMsg.getSize() <= currSize + 4)
			return false;
		this.ack.sendNext(ackMsg);
		return true;
	}

	public void sendNextExpectedNumber() throws Exception {
		Message msg = Message.newEmptyOutDataMessage(MessageType.SYN, to);
		msg.getHeaderBuffer().putInt(nextExpectedNumber);
		ack.sendNext(msg);
	}

	private void appendNack(Message msg) {
		if (nextExpectedNumber == (maxSeqRcvd + 1) && !recheck)
			return;
		recheck = false;

		ByteBuffer buff = msg.getHeaderBuffer();

		int diff = ack.max_size - msg.getSize();

		if (diff > 4) {
			buff.putInt(nextExpectedNumber);
			diff -= 4;
		}
		if (diff > NACK_SIZE) {
			int appended = 0;

			int from = -1;
			int to = -1;

			int count = 0;

			while (appended + NACK_SIZE < diff && count < rcvd.length) {
				int i = Math.abs(ackSenderCursor.getAndIncrement())
						% rcvd.length;
				count++;
				if (!rcvd[i].confirmed && rcvd[i].seq != -1) {
					int seq = rcvd[i].seq;
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

	public void receivedNackBuffer(ByteBuffer buffer) throws Exception {
		if (buffer.hasRemaining())
			sync(buffer.getInt(), false);
		while (buffer.hasRemaining()) {
			boolean type = buffer.getBoolean();
			if (type == RANGE) {
				int from = buffer.getInt();
				int to = buffer.getInt();
				if (log.isDebugEnabled())
					log.debug("Received NACK range :" + from + "-" + to);
				for (int i = from; i <= to; i++) {
					// resend(i);
					resendArray[pos(i)].resend = true;
				}
			} else {

				int from = buffer.getInt();
				if (log.isDebugEnabled())
					log.debug("Received NACK :" + from);
				resendArray[pos(from)].resend = true;
				// resend(from);
			}
		}

	}

	public void resend() throws Exception {
		if (seqN.get() == (confirmed.get() + 1))
			return;
		long curr = System.currentTimeMillis();
		for (int i = 0; i < resendArray.length; i++) {
			// if (log.isDebugEnabled())
			// log.debug("Resending " + seq);

			int seq = resendArray[i].seq;

			if (seq >= confirmed.get()) {

				Message data = resendArray[i].getData(seq);

				if (resendArray[i].resend
						&& data != null
						&& curr - resendArray[i].timeSent >= resendArray[i].timeout) {
					resendArray[i].timeSent = curr;
					resendArray[i].timeout *= timeout_mult;
					Message ackMsg = Message.encapsulateOut(data,
							MessageType.ACK_SEQ, to);
					ackMsg.getHeaderBuffer().putInt(seq);
					ackMsg.getHeaderBuffer().putInt(nextExpectedNumber);
					ack.sendNext(ackMsg);
				}
			}
		}

	}

	public void sync(int remoteNextExpected, boolean resend) throws Exception {
		if (remoteNextExpected == confirmed.get())
			return;

		for (int i = confirmed.get() + 1; i < remoteNextExpected; i++)
			confirm(i);

		if (resend) {
			// if (remoteNextExpected % 100 == 0)
			int seq = seqN.get();
			for (int i = remoteNextExpected; i < seq; i++) {
				// resend(i);
				resendArray[pos(i)].resend = true;
			}
		}
	}
}
