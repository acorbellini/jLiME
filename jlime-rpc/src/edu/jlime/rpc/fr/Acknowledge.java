package edu.jlime.rpc.fr;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.message.Header;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.ByteBuffer;

public class Acknowledge extends SimpleMessageProcessor {

	public static final int HEADER = Header.HEADER + 4;

	Object[] locks = new Object[1021];

	Logger log = Logger.getLogger(Acknowledge.class);

	ConcurrentHashMap<Address, AcknowledgeCounter> counters = new ConcurrentHashMap<>();

	CopyOnWriteArrayList<AcknowledgeCounter> counterList = new CopyOnWriteArrayList<>();

	private int max_size_nack;

	private int nack_delay;

	private int ack_delay;

	private Configuration config;

	// protected ConcurrentHashMap<Address, HashSet<Integer>> acks = new
	// ConcurrentHashMap<>();

	int max_size;

	private Timer t;

	public Acknowledge(MessageProcessor next, int max_size_nack,
			int nack_delay, int ack_delay, int max_size, Configuration config) {
		super(next, "Acknowledge");
		this.max_size_nack = max_size_nack;
		this.nack_delay = nack_delay;
		this.ack_delay = ack_delay;
		for (int i = 0; i < locks.length; i++) {
			locks[i] = new Object();
		}
		this.config = config;
		this.max_size = max_size;
	}

	@Override
	public void onStart() throws Exception {

		// Thread send = new Thread("Ack Sender") {
		// int cont = 0;
		//
		// @Override
		// public void run() {
		// while (!stopped) {
		// for (AcknowledgeCounter count : counterList) {
		// HashSet<Integer> list = count.getAcks();
		// // acks.get(count.to);
		// Message message = null;
		// while ((message = count.nextSend()) != null) {
		// try {
		// attachAcks(message, list);
		// sendNext(message);
		// cont = 0;
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// }
		// // List<Message> send2 = count.getSend();
		// // if (send2 != null)
		// // for (Message message : send2) {
		// //
		// // }
		// }
		// cont++;
		// if (cont == 1000)
		// synchronized (counters) {
		// cont = 0;
		// try {
		// counters.wait(200);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// }
		//
		// }
		// }
		// };
		// send.start();

		this.t = new Timer("Ack Timer");

		t.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				for (AcknowledgeCounter count : counterList) {
					count.resend();
					// HashSet<Integer> list = count.getAcks();
					// acks.get(count.to);

				}
			}
		}, config.retransmit_delay, config.retransmit_delay);

		t.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				for (AcknowledgeCounter count : counterList) {
					// for (Entry<Address, HashSet<Integer>> e :
					// acks.entrySet()) {
					count.sendAcks();
				}
			}
		}, config.ack_delay, config.ack_delay);

		getNext().addMessageListener(MessageType.ACK_SEQ,
				new MessageListener() {
					@Override
					public void rcv(Message m, MessageProcessor origin)
							throws Exception {
						ByteBuffer headerBuffer = m.getHeaderBuffer();
						int seq = headerBuffer.getInt();
						if (log.isTraceEnabled())
							log.trace("Received Ack'd msg with seq # " + seq
									+ " from " + m.getFrom());

						AcknowledgeCounter counter = getCounter(m.getFrom());

						if (counter.seqNumberArrived(seq)) {
							notifyRcvd(Message.deEncapsulate(
									m.getDataAsBytes(), m.getFrom(), m.getTo()));
							// Address from = m.getFrom();

							// counters.get(from).addAck(seq);

						}
						receivedAckBuffer(m.getFrom(), headerBuffer);

						// Message ackMsg = Message.newEmptyOutDataMessage(
						// MessageType.ACK, m.getFrom());
						// ackMsg.getHeaderBuffer().putInt(seq);
						// sendNext(ackMsg);
					}
				});

		getNext().addMessageListener(MessageType.ACK, new MessageListener() {
			@Override
			public void rcv(Message m, MessageProcessor origin)
					throws Exception {
				receivedAckBuffer(m.getFrom(), m.getHeaderBuffer());
				// int ackConfirm = m.getHeaderBuffer().getInt();
				// if (log.isTraceEnabled())
				// log.trace("Received message from " + m.getFrom()
				// + " that confirms " + ackConfirm);
				// AcknowledgeCounter c = getCounter(m.getFrom());
				// c.confirm(ackConfirm);
				// synchronized (counters) {
				// counters.notifyAll();
				// }
			}
		});
	}

	@Override
	public void send(Message msg) throws Exception {
		AcknowledgeCounter c = getCounter(msg.getTo());

		c.send(msg);
		synchronized (counters) {
			counters.notifyAll();
		}
	}

	private AcknowledgeCounter getCounter(Address to) throws Exception {
		AcknowledgeCounter counter = counters.get(to);
		if (counter == null) {
			synchronized (counters) {
				counter = counters.get(to);
				if (counter == null) {
					counter = new AcknowledgeCounter(this, to, max_size_nack,
							nack_delay, ack_delay, config);
					counters.put(to, counter);
					counterList.add(counter);
				}
			}
		}
		return counter;

	}

	@Override
	public void onStop() throws Exception {
		t.cancel();
	}

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		AcknowledgeCounter count = counters.remove(addr);
		if (count != null)
			counterList.remove(count);
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}

	private void receivedAckBuffer(Address from, ByteBuffer headerBuffer)
			throws Exception {
		if (headerBuffer.hasRemaining()) {
			int acksCount = headerBuffer.getInt();
			for (int i = 0; i < acksCount; i++) {
				int seq = headerBuffer.getInt();
				AcknowledgeCounter c = getCounter(from);
				c.confirm(seq);
			}
			// synchronized (counters) {
			// counters.notifyAll();
			// }
		}

	}

}
