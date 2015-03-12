package edu.jlime.rpc.fr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.fr.NACKAcknowledge.Nack;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageType;

class NACKAcknowledgeCounter {

	private Logger log = Logger.getLogger(NACKAcknowledgeCounter.class);

	Address to;

	boolean[] rcvd;

	private volatile int nextExpectedNumber = 0;

	private AtomicReferenceArray<ResendData> resendArray;

	private volatile AtomicInteger seqN = new AtomicInteger(0);

	private int max_resend_size = 256;

	private volatile int confirmed = -1;

	private ConcurrentLinkedDeque<Message> toSend = new ConcurrentLinkedDeque<>();

	private Configuration config;

	private volatile long current_ack_time;

	private float alpha = 0.5f;

	private ConcurrentHashMap<Integer, Nack> nacks = new ConcurrentHashMap<>();

	public static class ResendData {

		byte[] data;

		long timeSent;

		int seq;

		public volatile boolean confirmed = false;

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

		@Override
		public String toString() {
			return "ResendData [data=" + data.length + ", timeSent=" + timeSent
					+ ", seq=" + seq + ", confirmed=" + confirmed + "]";
		}

	}

	public NACKAcknowledgeCounter(Address to, int max_nack_size,
			int nack_delay, int ack_delay, Configuration config) {
		this.max_resend_size = config.ack_max_resend_size;
		resendArray = new AtomicReferenceArray<>(max_resend_size);
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
			for (int i = nextExpectedNumber; i < seq; i++) {
				nacks.put(i, new Nack(i, false));
			}

		}
		return true;

	}

	private int pos(int seqN) {
		return seqN % max_resend_size;
	}

	public synchronized void confirm(int max) throws Exception {
		for (int seq = confirmed + 1; seq <= max; seq++) {
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
			} else {
				curr.setConfirmed();

				// System.out.println("Confirm " + seq
				// + " removing from nack list.");
				nacks.remove(seq);

				current_ack_time = (long) (alpha
						* (System.currentTimeMillis() - curr.timeSent)
						+ (1 - alpha) * current_ack_time + 0.1f * config.ack_delay);
			}

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
						// System.out.println("Confirm " + confirmed
						// + " removing from nack list.");
						nacks.remove(confirmed);
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
	}

	public List<Message> getSend() {
		ArrayList<Message> ret = new ArrayList<>();
		loadNewMessages(ret);

		// loadResendMessages(ret);

		return ret;
	}

	public ArrayList<Message> getResend(List<Nack> list) {

		ArrayList<Message> ret = new ArrayList<Message>();

		for (Nack nack : list) {
			loadSeq(ret, pos(nack.seq));
			if (nack.last) {
				for (int i = nack.seq; i < seqN.get(); i++) {
					loadSeq(ret, pos(i));
				}
			}
		}
		return ret;
	}

	private void loadSeq(ArrayList<Message> ret, int pos) {
		ResendData res = resendArray.get(pos);

		if (res != null && !res.isConfirmed()
		// current_ack_time
		) {
			try {
				if (log.isDebugEnabled())
					log.debug("Resending Seq " + res.seq);
				Message ackMsg = Message.newOutDataMessage(res.data,
						MessageType.ACK_SEQ, to);
				ackMsg.getHeaderBuffer().putInt(res.seq);

				if (!res.isConfirmed())
					ret.add(ackMsg);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void loadNewMessages(ArrayList<Message> ret) {

		while (!toSend.isEmpty()
				&& Math.abs(seqN.get() - confirmed) <= max_resend_size) {

			int seqN = this.seqN.getAndIncrement();

			Message msg = toSend.pop();

			resendArray.set(
					pos(seqN),
					new ResendData(msg.toByteArray(), System
							.currentTimeMillis(), seqN));

			Message ackMsg = Message.encapsulateOut(msg, MessageType.ACK_SEQ,
					to);

			ackMsg.getHeaderBuffer().putInt(seqN);
			ret.add(ackMsg);
		}
	}

	public List<Nack> getNACK() {
		ArrayList<Nack> list = new ArrayList<>(nacks.values());
		list.add(new Nack(nextExpectedNumber, true));
		return list;
	}
}