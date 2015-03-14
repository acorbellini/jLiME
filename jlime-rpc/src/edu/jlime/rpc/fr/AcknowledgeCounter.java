package edu.jlime.rpc.fr;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.mina.util.ConcurrentHashSet;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.RingQueue;
import gnu.trove.list.array.TIntArrayList;

class AcknowledgeCounter {

	public static class ConfirmData {
		volatile boolean confirmed = false;
		volatile int seq = -1;
		public boolean ackSent = false;
	}

	private Logger log = Logger.getLogger(AcknowledgeCounter.class);

	Address to;

	ConfirmData[] rcvd;

	private volatile int nextExpectedNumber = 0;

	private ResendData[] resendArray;

	private volatile AtomicInteger seqN = new AtomicInteger(0);

	private int max_resend_size = 16;

	private volatile AtomicInteger confirmed = new AtomicInteger(-1);

	private RingQueue toSend = new RingQueue();

	private Configuration config;

	// private volatile long current_ack_time;
	//
	// private float alpha = 0.5f;

	// private LinkedList<Integer> acks = new LinkedList<>();

	private Acknowledge ack;

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

	public AcknowledgeCounter(Acknowledge ack, Address to, int max_nack_size,
			int nack_delay, int ack_delay, Configuration config) {
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
		// Arrays.fill(rcvd, false);
		this.config = config;
		// this.current_ack_time = config.ack_delay;
	}

	public void send(Message msg) throws Exception {
		// toSend.put(msg);
		int seqN = this.seqN.getAndIncrement();
		while (Math.abs(seqN - confirmed.get()) > max_resend_size) {
			synchronized (this.seqN) {
				this.seqN.wait(0, 500);
			}
		}
		// if (ret == null)
		// ret = new ArrayList<>();

		// Message msg = (Message) toSend.tryTakeOne();
		// if (msg == null)
		// return null;

		resendArray[pos(seqN)].setData(msg.toByteArray(),
				System.currentTimeMillis(), seqN);

		Message ackMsg = Message.encapsulateOut(msg, MessageType.ACK_SEQ, to);
		ackMsg.getHeaderBuffer().putInt(seqN);
		ack.sendNext(ackMsg);
		// ret.add(ackMsg);
		// return ackMsg;
	}

	public boolean seqNumberArrived(int seq) throws Exception {
		if (seq < nextExpectedNumber) {
			rcvd[pos(seq)].ackSent = false;
			return false;
		}

		rcvd[pos(seq)].confirmed = true;
		rcvd[pos(seq)].ackSent = false;
		rcvd[pos(seq)].seq = seq;

		if (seq == nextExpectedNumber) {
			synchronized (rcvd) {
				if (seq == nextExpectedNumber) {
					while (rcvd[pos(nextExpectedNumber)].confirmed) {
						rcvd[pos(nextExpectedNumber)].confirmed = false;
						nextExpectedNumber++;
					}
				}
			}
		}

		return true;

	}

	private int pos(int seqN) {
		return seqN % max_resend_size;
	}

	public void confirm(int seq) throws Exception {
		if (seq <= confirmed.get())
			return;

		ResendData curr = resendArray[pos(seq)];
		curr.setConfirmed();
		// current_ack_time = (long) (alpha
		// * (System.currentTimeMillis() - curr.timeSent) + (1 - alpha)
		// * current_ack_time + 0.1f * config.ack_delay);

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
					// synchronized (this) {
					// confirmed.notifyAll();
					// }
				}
			}
		}
	}

	public Message nextSend() {
		// ArrayList<Message> ret = null;
		Message ret = null;
		if (Math.abs(seqN.get() - confirmed.get()) <= max_resend_size) {
			// if (ret == null)
			// ret = new ArrayList<>();

			Message msg = (Message) toSend.tryTakeOne();
			if (msg == null)
				return null;

			int seqN = this.seqN.getAndIncrement();
			resendArray[pos(seqN)].setData(msg.toByteArray(),
					System.currentTimeMillis(), seqN);

			Message ackMsg = Message.encapsulateOut(msg, MessageType.ACK_SEQ,
					to);
			ackMsg.getHeaderBuffer().putInt(seqN);
			// ret.add(ackMsg);
			return ackMsg;

		}
		return ret;
	}

	// public HashSet<Integer> getAcks() {
	// return acks;
	// }

	// public void addAck(int seq) {
	// synchronized (acks) {
	// acks.add(seq);
	// }
	// }

	public TIntArrayList getAcks(int currSize) {
		// ByteBuffer buff = msg.getHeaderBuffer();
		int size = currSize;
		int diff = (ack.max_size - 4) - size;
		int count = diff / 16;
		if (count > 0) {
			TIntArrayList list = new TIntArrayList();
			for (int i = 0; i < rcvd.length && list.size() < count; i++) {
				if (!rcvd[i].ackSent) {
					rcvd[i].ackSent = true;
					list.add(rcvd[i].seq);
				}
			}
			return list;

		}
		return null;
	}

	public void sendAcks() {
		TIntArrayList list = getAcks(0);
		if (list == null || list.isEmpty())
			return;
		Message ack = Message.newEmptyOutDataMessage(MessageType.ACK, to);
		appendAcks(ack, list);
		try {
			this.ack.sendNext(ack);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	public void resend() {
		long curr = System.currentTimeMillis();
		for (int i = 0; i < max_resend_size; i++) {
			ResendData res = resendArray[i];

			boolean confirmed = res.confirmed;

			byte[] data = res.data;

			long time = res.timeSent;

			int seq = res.seq;

			if (data != null && !confirmed
					&& curr - time > config.retransmit_delay) {
				try {
					res.timeSent = curr;

					Message ackMsg = Message.newOutDataMessage(data,
							MessageType.ACK_SEQ, to);
					ackMsg.getHeaderBuffer().putInt(seq);
					TIntArrayList list = getAcks(ackMsg.getSize());
					if (list != null && !list.isEmpty()) {
						appendAcks(ackMsg, list);
					}
					if (!confirmed)
						ack.sendNext(ackMsg);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void appendAcks(Message ackMsg, TIntArrayList list) {
		ByteBuffer buff = ackMsg.getHeaderBuffer();
		buff.ensureCapacity(list.size() * 4 + 4);
		buff.putInt(list.size());
		for (int j = 0; j < list.size(); j++) {
			buff.putInt(list.get(j));
		}
	}
}