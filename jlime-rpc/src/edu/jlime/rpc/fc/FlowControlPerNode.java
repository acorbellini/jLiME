package edu.jlime.rpc.fc;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.ByteBuffer;

class FlowControlPerNode extends SimpleMessageProcessor {

	private Logger log = Logger.getLogger(FlowControlPerNode.class);

	private int currSend = 0;

	private int currRcvd = 0;

	int max_send;

	FCConfiguration config;

	private Address addr;

	private Timer t;

	public FlowControlPerNode(Address to, MessageProcessor comm,
			final FCConfiguration config) {
		super(comm, "Flow Control for " + to);
		this.addr = to;
		this.config = config;
		this.max_send = config.max_send_initial;
		t = new Timer("Resend ack from flow control to " + to);
		t.schedule(
				new TimerTask() {
					@Override
					public void run() {
						try {

							long curr = System.currentTimeMillis();
							if (curr - lastFCAckSent < config.time_before_resend_ack)
								return;
							if (log.isDebugEnabled())
								log.debug("Sending FC_ACK, passed "
										+ config.time_before_resend_ack / 1000
										+ " sec.");
							sendFCAck();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}, this.config.time_before_resend_ack,
				this.config.time_before_resend_ack);
	}

	Long lastWaitTime;

	Long lastLatency = null;

	protected synchronized void ackRcvd() {
		if (lastWaitTime != null) {
			long curr = System.currentTimeMillis();
			long time = curr - lastWaitTime;
			if (lastLatency != null)
				updateMaxSend(time, lastLatency);
			lastLatency = time;
			lastWaitTime = null;
		}

		currSend = 0;
		notifyAll();
	}

	private void updateMaxSend(Long currLatency, Long lastLatency) {

		float improvement = config.movement_factor
				+ (lastLatency / (float) currLatency);

		int old_max_send = max_send;

		max_send = (int) (max_send * config.old_send_importance + config.new_send_importance
				* max_send * improvement);

		if (log.isDebugEnabled())
			log.debug("Current latency is " + currLatency
					+ " last latency was " + lastLatency
					+ " Updating Max Send " + old_max_send + " to " + max_send);

		if (max_send < config.min_send_threshold)
			max_send = config.min_send_threshold;
	}

	public synchronized void getPermission(int size)
			throws InterruptedException {
		while (currSend + size >= max_send) {
			log.debug("Message of size " + size + " passed size of Max send ("
					+ max_send + "b) Current Send: " + currSend);
			lastWaitTime = System.currentTimeMillis();
			wait();
		}
		currSend += size;
	}

	final Semaphore initSem = new Semaphore(0);

	public synchronized int getMaxSend() {
		return max_send;
	}

	@Override
	public void onStop() throws Exception {
		t.cancel();
	}

	private Semaphore lockWait = new Semaphore(1);

	@Override
	public void send(Message msg) throws Exception {
		Message fc_msg = Message.encapsulateOut(msg, MessageType.FC, addr);
		lockWait.acquire();
		getPermission(fc_msg.getDataSize());
		lockWait.release();

		ByteBuffer writer = fc_msg.getHeaderBuffer();
		writer.putInt(max_send);
		sendNext(fc_msg);
	}

	boolean first = true;

	private long lastFCAckSent = System.currentTimeMillis();

	public synchronized void update(int rcvd, int max_send_remote)
			throws Exception {

		currRcvd += rcvd;
		// if (first && max_send_remote != max_rcv) {
		// if (log.isDebugEnabled())
		// log.debug("First message in FC: Remote peer " + addr
		// + " had a wrong send rate(" + max_send_remote
		// + "b), sending ack to update (" + max_rcv + "b).");
		// sendFCAck();
		// first = false;
		// return; // For now, I don't do anything
		// }
		if (currRcvd >= config.send_ack_threshold * max_send_remote) {
			if (log.isDebugEnabled())
				log.debug("FC passed " + config.send_ack_threshold + " of "
						+ max_send_remote + " sending FC_ACK");
			sendFCAck();
			currRcvd = 0;
		}

	}

	private void sendFCAck() throws Exception {

		lastFCAckSent = System.currentTimeMillis();
		Message msg = Message.newEmptyOutDataMessage(MessageType.FC_ACK, addr);
		sendNext(msg);
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}

}