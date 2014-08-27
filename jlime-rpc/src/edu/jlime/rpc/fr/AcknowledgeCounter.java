package edu.jlime.rpc.fr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageType;

class AcknowledgeCounter {

	private Logger log = Logger.getLogger(AcknowledgeCounter.class);

	private Address to;

	boolean[] rcvd;

	private volatile int nextExpectedNumber = 0;

	private AtomicReferenceArray<ResendData> resendArray;

	private volatile int seqN = 0;

	private int max_resend_size = 16;

	private volatile int confirmed = -1;

	private ConcurrentLinkedDeque<Message> toSend = new ConcurrentLinkedDeque<>();

	public static class ResendData {

		byte[] data;

		long timeSent;

		int seq;

		public boolean confirmed = false;

		public ResendData(byte[] data, long timeSent, int seq) {
			super();
			this.data = data;
			this.timeSent = timeSent;
			this.seq = seq;
		}

		public void setConfirmed() {
			this.confirmed = true;
			this.data = null;
		}

		public boolean isConfirmed() {
			return confirmed;
		}
	}

	public AcknowledgeCounter(Address to, int max_nack_size, int nack_delay,
			int ack_delay) {
		resendArray = new AtomicReferenceArray<>(max_resend_size);
		this.to = to;
		rcvd = new boolean[max_resend_size];
		Arrays.fill(rcvd, false);
	}

	public void send(Message msg) throws Exception {

		toSend.add(msg);
	}

	public synchronized boolean seqNumberArrived(int seq) throws Exception {
		// sendAck(seq);
		if (seq < nextExpectedNumber) {
			if (log.isDebugEnabled())
				log.debug("Seq " + seq + " is less than " + nextExpectedNumber
						+ ", not updating (RESEND?).");
			return false;
		}
		rcvd[pos(seq)] = true;
		if (seq == nextExpectedNumber) {
			// (this) {
			// if (seq == nextExpectedNumber) {
			if (log.isDebugEnabled())
				log.debug("Consecutive " + seq + " arrived, with respect to "
						+ nextExpectedNumber);

			while (rcvd[pos(nextExpectedNumber)]) {
				rcvd[pos(nextExpectedNumber)] = false;
				nextExpectedNumber++;
			}
			if (log.isDebugEnabled())
				log.debug("Validated from " + seq + " to "
						+ (nextExpectedNumber - 1) + ". Next Expected : "
						+ nextExpectedNumber);
			// }
			// }

		} else if (seq > nextExpectedNumber) {
			if (log.isDebugEnabled())
				log.info("Non Consecutive Seq " + seq + " with respect to "
						+ nextExpectedNumber);
		}
		return true;

	}

	private int pos(int seqN) {
		return seqN % max_resend_size;
	}

	public synchronized void confirm(int seq) throws Exception {
		if (seq <= confirmed) {
			if (log.isDebugEnabled())
				log.debug("Ignoring ACK Minimum Consecutive Seq Number Confirmed is "
						+ confirmed + " and ack received is " + seq);
			return;
		} else if (seq > confirmed + 1)
			if (log.isDebugEnabled())
				log.debug("Received ACK out of order Minimum Consecutive Seq Number Confirmed is "
						+ confirmed + " and ack received is " + seq);

		ResendData curr = resendArray.get(pos(seq));
		if (curr == null) {
			if (log.isDebugEnabled())
				log.debug("Received ACK for already confirmed Seq number "
						+ seq + " confirmed so far is " + confirmed);
		} else
			curr.setConfirmed();

		if (seq == confirmed + 1) {
			// if (log.isDebugEnabled())
			// log.debug("Updating Minimum Consecutive Seq Number Confirmed is "
			// + confirmed + " and ack received is " + seq);
			confirmed++;
			resendArray.set(pos(confirmed), null);
			boolean done = false;
			while (!done) {
				ResendData res = resendArray.get(pos(confirmed + 1));
				if (res != null && res.isConfirmed()) {
					confirmed++;
					resendArray.set(pos(confirmed), null);
				} else
					done = true;
			}
			synchronized (this) {
				notifyAll();
			}
			if (log.isDebugEnabled())
				log.debug("Updated Minimum Consecutive Seq Number Confirmed to "
						+ confirmed + " and ack received is " + seq);
		}
	}

	public List<Message> getSend() {
		ArrayList<Message> ret = new ArrayList<>();
		while (!toSend.isEmpty() && seqN - confirmed <= max_resend_size) {
			Message msg = toSend.pop();

			resendArray.set(
					pos(seqN),
					new ResendData(msg.toByteArray(), System
							.currentTimeMillis(), seqN));

			Message ackMsg = Message.encapsulateOut(msg, MessageType.ACK_SEQ,
					to);

			ackMsg.getHeaderBuffer().putInt(seqN);

			synchronized (this) {
				seqN++;
			}

			ret.add(ackMsg);
		}

		long curr = System.currentTimeMillis();
		for (int i = 0; i < max_resend_size; i++) {
			ResendData res = resendArray.get(i);
			if (res != null && !res.isConfirmed() && curr - res.timeSent > 1000) {
				try {
					res.timeSent = curr;
					if (log.isDebugEnabled())
						log.debug("Resending Seq " + res.seq);
					Message ackMsg = Message.newOutDataMessage(res.data,
							MessageType.ACK_SEQ, to);
					ackMsg.getHeaderBuffer().putInt(res.seq);
					ret.add(ackMsg);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return ret;
	}
}