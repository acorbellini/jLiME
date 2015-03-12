package edu.jlime.rpc.fr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageType;

class AcknowledgeCounter {

	private Logger log = Logger.getLogger(AcknowledgeCounter.class);

	Address to;

	boolean[] rcvd;

	private volatile int nextExpectedNumber = 0;

	private ResendData[] resendArray;

	private volatile AtomicInteger seqN = new AtomicInteger(0);

	private int max_resend_size = 16;

	private volatile AtomicInteger confirmed = new AtomicInteger(-1);

	private ConcurrentLinkedDeque<Message> toSend = new ConcurrentLinkedDeque<>();

	private Configuration config;

	private volatile long current_ack_time;

	private float alpha = 0.5f;

	private HashSet<Integer> acks = new HashSet<>();

	public static class ResendData {

		volatile byte[] data = null;

		volatile long timeSent = -1;

		volatile int seq = -1;

		volatile boolean confirmed = false;

		public void setConfirmed() {
			this.confirmed = true;
		}

		public boolean isConfirmed() {
			return confirmed;
		}

		void setData(byte[] data, long timeSent, int seq) {
			this.confirmed = false;
			this.timeSent = timeSent;
			this.seq = seq;
			this.data = data;
		}
	}

	public AcknowledgeCounter(Address to, int max_nack_size, int nack_delay,
			int ack_delay, Configuration config) {
		this.max_resend_size = config.ack_max_resend_size;
		resendArray = new ResendData[max_resend_size];
		for (int i = 0; i < resendArray.length; i++) {
			resendArray[i] = new ResendData();
		}
		this.to = to;
		rcvd = new boolean[max_resend_size];
		Arrays.fill(rcvd, false);
		this.config = config;
		this.current_ack_time = config.ack_delay;
	}

	public void send(Message msg) throws Exception {
		toSend.add(msg);
	}

	public synchronized boolean seqNumberArrived(int seq) throws Exception {
		// sendAck(seq);
		if (seq < nextExpectedNumber) {
			// if (log.isDebugEnabled())
			// log.debug("Seq " + seq + " is less than " + nextExpectedNumber
			// + ", not updating (RESEND?).");
			return false;
		}
		rcvd[pos(seq)] = true;
		if (seq == nextExpectedNumber) {
			// (this) {
			// if (seq == nextExpectedNumber) {
			// if (log.isDebugEnabled())
			// log.debug("Consecutive " + seq + " arrived, with respect to "
			// + nextExpectedNumber);

			while (rcvd[pos(nextExpectedNumber)]) {
				rcvd[pos(nextExpectedNumber)] = false;
				nextExpectedNumber++;
			}
			// if (log.isDebugEnabled())
			// log.debug("Validated from " + seq + " to "
			// + (nextExpectedNumber - 1) + ". Next Expected : "
			// + nextExpectedNumber);
			// }
			// }

		} else if (seq > nextExpectedNumber) {
			// if (log.isDebugEnabled())
			// log.info("Non Consecutive Seq " + seq + " with respect to "
			// + nextExpectedNumber);
		}
		return true;

	}

	private int pos(int seqN) {
		return seqN % max_resend_size;
	}

	public void confirm(int seq) throws Exception {
		// log.info("ACK number for " + to + " - " + seq +
		// " confirmed so far is "
		// + confirmed);
		if (seq <= confirmed.get()) {
			// if (log.isDebugEnabled())
			// log.debug("Ignoring ACK Minimum Consecutive Seq Number Confirmed is "
			// + confirmed + " and ack received is " + seq);
			return;
		}
		// else if (seq > confirmed.get() + 1)
		// if (log.isDebugEnabled())
		// log.debug("Received ACK out of order Minimum Consecutive Seq Number Confirmed is "
		// + confirmed + " and ack received is " + seq)
		;

		ResendData curr = resendArray[pos(seq)];
		if (curr == null) {
			// if (log.isDebugEnabled())
			// log.info("Received ACK for already confirmed Seq number " + seq
			// + " confirmed so far is " + confirmed);
		} else {
			curr.setConfirmed();
			current_ack_time = (long) (alpha
					* (System.currentTimeMillis() - curr.timeSent)
					+ (1 - alpha) * current_ack_time + 0.1f * config.ack_delay);
		}

		if (seq == confirmed.get() + 1) {
			synchronized (this) {
				if (seq == confirmed.get() + 1) {
					// if (log.isDebugEnabled())
					// log.debug("Updating Minimum Consecutive Seq Number Confirmed is "
					// + confirmed + " and ack received is " + seq);
					// resendArray.set(pos(confirmed.get() + 1), null);
					resendArray[pos(confirmed.get() + 1)].data = null;
					confirmed.incrementAndGet();

					boolean done = false;
					while (!done) {
						ResendData res = resendArray[pos(confirmed.get() + 1)];
						if (res.data != null && res.isConfirmed()) {
							// resendArray.set(pos(confirmed.get() + 1), null);
							res.data = null;
							confirmed.incrementAndGet();
						} else
							done = true;
					}
					synchronized (this) {
						notifyAll();
					}
				}
			}
			// if (log.isDebugEnabled())
			// log.debug("Updated Minimum Consecutive Seq Number Confirmed to "
			// + confirmed + " and ack received is " + seq);
		}
	}

	public List<Message> getSend() {
		ArrayList<Message> ret = null;
		while (!toSend.isEmpty()
				&& Math.abs(seqN.get() - confirmed.get()) <= max_resend_size) {
			if (ret == null)
				ret = new ArrayList<>();
			int seqN = this.seqN.getAndIncrement();

			Message msg = toSend.pop();

			resendArray[pos(seqN)].setData(msg.toByteArray(),
					System.currentTimeMillis(), seqN);

			Message ackMsg = Message.encapsulateOut(msg, MessageType.ACK_SEQ,
					to);

			ackMsg.getHeaderBuffer().putInt(seqN);
			ret.add(ackMsg);
		}
		return ret;
	}

	public synchronized ArrayList<Message> getResend() {
		ArrayList<Message> ret = null;
		long curr = System.currentTimeMillis();
		for (int i = 0; i < max_resend_size; i++) {
			if (ret == null)
				ret = new ArrayList<Message>();
			ResendData res = resendArray[i];

			boolean confirmed = res.confirmed;

			byte[] data = res.data;

			long time = res.timeSent;

			int seq = res.seq;

			if (data != null && !confirmed
					&& curr - time > config.retransmit_delay
			// current_ack_time
			) {
				try {
					res.timeSent = curr;

					// if (log.isDebugEnabled())
					// log.debug("Resending Seq " + seq);

					Message ackMsg = Message.newOutDataMessage(data,
							MessageType.ACK_SEQ, to);
					ackMsg.getHeaderBuffer().putInt(seq);

					if (!confirmed)
						ret.add(ackMsg);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return ret;
	}

	public HashSet<Integer> getAcks() {
		return acks;
	}

	public void addAck(int seq) {
		synchronized (acks) {
			acks.add(seq);
		}
	}
}