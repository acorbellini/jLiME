package edu.jlime.rpc.fr;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.NetworkConfiguration;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.util.ByteBuffer;

class AcknowledgeCounter {

	private static final int ACK_HEADER = 1 + 4 + 4;

	private Logger log = Logger.getLogger(AcknowledgeCounter.class);

	Address to;

	ConfirmData[] rcvd;

	private volatile int nextExpectedNumber = 0;

	private ResendData[] resendArray;

	private volatile AtomicInteger seqN = new AtomicInteger(0);

	private int max_resend_size;

	private volatile AtomicInteger confirmed = new AtomicInteger(-1);

	private volatile AtomicInteger resendCursor = new AtomicInteger(0);

	private NetworkConfiguration config;

	private Acknowledge ack;

	// private LinkedBlockingDeque<Integer> acks = new
	// LinkedBlockingDeque<Integer>();

	Message ackMsg;

	private volatile long maxSeqRcvd = -1l;

	private float timeout_mult;

	public AcknowledgeCounter(Acknowledge ack, Address to,
			NetworkConfiguration config) {
		this.ack = ack;
		this.timeout_mult = config.timeout_mult;
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
		// this.ackSent = new AtomicIntegerArray(max_resend_size);
		// for (int i = 0; i < ackSent.length(); i++) {
		// ackSent.set(i, 0);
		// }

		this.config = config;
		this.ackMsg = Message.newEmptyOutDataMessage(MessageType.ACK, to);

	}

	public void send(Message msg) throws Exception {
		int seqN = this.seqN.getAndIncrement();
		while (Math.abs(seqN - confirmed.get()) >= max_resend_size) {
			synchronized (this.seqN) {
				this.seqN.wait(0, 500);
			}
		}

		if (log.isDebugEnabled())
			log.debug("Sending message with seqN " + seqN + " to "
					+ msg.getTo());

		resendArray[pos(seqN)].setData(msg, System.currentTimeMillis(), seqN,
				config.retransmit_delay);

		Message ackSeqMsg = Message
				.encapsulateOut(msg, MessageType.ACK_SEQ, to);
		ackSeqMsg.getHeaderBuffer().putInt(seqN);
		ackSeqMsg.getHeaderBuffer().putInt(nextExpectedNumber);
		// appendAcks(ackSeqMsg, 10, 10);

		int size = ackSeqMsg.getSize();
		if (size > config.max_msg_size)
			log.warn("Surpasing max message size " + config.max_msg_size
					+ " with " + size);

		ack.sendNext(ackSeqMsg);
	}

	public synchronized boolean seqNumberArrived(int seq) throws Exception {
		if (seq < nextExpectedNumber || rcvd[pos(seq)].confirmed) {
			rcvd[pos(seq)].ackSent = false;
			// acks.add(seq);
			recheck = true;
			if (log.isDebugEnabled())
				log.debug("Ignoring repeated seq number " + seq
						+ " next expected " + nextExpectedNumber + " from "
						+ to);
			return false;
		}

		maxSeqRcvd = Math.max(maxSeqRcvd, seq);

		if (log.isDebugEnabled())
			log.debug("Received seq number " + seq + " from " + to);

		rcvd[pos(seq)].confirmed = true;
		rcvd[pos(seq)].seq = seq;
		rcvd[pos(seq)].ackSent = false;

		if (seq == nextExpectedNumber) {
			while (rcvd[pos(nextExpectedNumber)].confirmed) {
				rcvd[pos(nextExpectedNumber)].ackSent = true;
				rcvd[pos(nextExpectedNumber)].confirmed = false;
				// rcvd[pos(nextExpectedNumber)].seq = -1;
				nextExpectedNumber++;
			}
		}

		return true;

	}

	private int pos(int seqN) {
		return seqN % max_resend_size;
	}

	public void confirm(int seq) throws Exception {
		if (seq <= confirmed.get()) {
			if (log.isDebugEnabled())
				log.debug("Ignoring confirmation for seq number " + seq
						+ " from " + to);
			return;
		}

		if (log.isDebugEnabled())
			log.debug("Received confirmation for seq number " + seq + " from "
					+ to);

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
					if (log.isDebugEnabled())
						log.debug("Confirmed " + seq + " updated confirmed "
								+ confirmed.get() + " for address " + to);
				}
			}
		}

	}

	public boolean sendAcks() throws Exception {
		ackMsg.getHeader().clear();

		int currSize = ackMsg.getSize();

		appendAcks(ackMsg);

		if (ackMsg.getSize() <= currSize + 4)
			return false;
		this.ack.sendNext(ackMsg);
		return true;
	}

	public void resend() {

		if (seqN.get() == (confirmed.get() + 1))
			return;

		long curr = System.currentTimeMillis();
		int min = 0;// confirmed.get() + 1;
		int max = resendArray.length - 1;// seqN.get();
		int sent = 0;
		int count = min;
		// int maxSent = max_resend_size / 4;
		while (count <= max
		// && sent <= maxSent
		// && count < MAX_RESEND_ITERATIONS
		) {
			int i = Math.abs(resendCursor.getAndIncrement())
					% resendArray.length;

			ResendData res = resendArray[i];

			boolean confirmed = res.confirmed;

			Message data = res.data;

			long time = res.timeSent;

			int seq = res.seq;

			if (data != null && !confirmed && curr - time > res.timeout) {
				try {
					res.timeSent = curr;
					res.timeout *= timeout_mult;
					Message ackMsg = Message.encapsulateOut(data,
							MessageType.ACK_SEQ, to);
					ackMsg.getHeaderBuffer().putInt(seq);
					ackMsg.getHeaderBuffer().putInt(nextExpectedNumber);

					// appendAcks(ackMsg, 100, 100);

					if (!confirmed) {
						if (log.isDebugEnabled())
							log.debug("Resending message with seq" + seq
									+ " to " + to);
						ack.sendNext(ackMsg);
						sent++;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			count++;
		}
	}

	final boolean RANGE = true;
	final boolean SINGLE = false;

	private AtomicInteger ackSenderCursor = new AtomicInteger(0);

	private volatile boolean recheck = false;

	private void appendAcks(Message msg) {
		if (nextExpectedNumber == (maxSeqRcvd + 1) && !recheck)
			return;

		recheck = false;

		ByteBuffer buff = msg.getHeaderBuffer();

		int diff = ack.max_size - msg.getSize();

		if (diff > 4) {
			buff.putInt(nextExpectedNumber);
			diff -= 4;
		}
		if (diff > ACK_HEADER) {
			int appended = 0;

			int from = -1;
			int to = -1;

			int count = 0;

			while (appended + ACK_HEADER < diff && count < rcvd.length) {
				int i = Math.abs(ackSenderCursor.getAndIncrement()
						% rcvd.length);

				count++;

				int seq = rcvd[i].seq;

				if (!rcvd[i].ackSent) {
					rcvd[i].ackSent = true;
					if (from == -1) {
						from = seq;
						to = seq;
					} else if (to != seq - 1) {
						if (to != from) {
							buff.putBoolean(RANGE);
							buff.putInt(from);
							buff.putInt(to);
							appended += ACK_HEADER;
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

				// ackSent.compareAndSet(i, 0, 1);
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

	public void receivedAckBuffer(ByteBuffer headerBuffer) throws Exception {
		while (headerBuffer.hasRemaining()) {
			boolean type = headerBuffer.getBoolean();
			if (type == RANGE) {
				int from = headerBuffer.getInt();
				int to = headerBuffer.getInt();
				for (int i = from; i <= to; i++) {
					confirm(i);
				}
			} else {
				int from = headerBuffer.getInt();
				confirm(from);
			}
		}
	}

	public void confirmAll(int remoteNextExpected) throws Exception {
		if (remoteNextExpected <= confirmed.get())
			return;
		if (log.isDebugEnabled())
			log.debug("Confirming all [" + confirmed.get() + 1 + ","
					+ remoteNextExpected + ") from " + to);
		for (int i = confirmed.get() + 1; i < remoteNextExpected; i++) {
			confirm(i);
		}
	}
}
