package edu.jlime.rpc.fr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

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

public class NACKAcknowledge extends SimpleMessageProcessor {

	public static final int HEADER = Header.HEADER + 4;

	Object[] locks = new Object[1021];

	Logger log = Logger.getLogger(NACKAcknowledge.class);

	ConcurrentHashMap<Address, NACKAcknowledgeCounter> counters = new ConcurrentHashMap<>();

	public static class Nack {
		public Nack(int seq2, boolean lastReceived) {
			this.seq = seq2;
			this.last = lastReceived;
		}

		int seq;
		boolean last = false;

		@Override
		public String toString() {
			return "[" + seq + "," + last + "]";
		}
	}

	private int max_size_nack;

	private int nack_delay;

	private int ack_delay;

	private Configuration config;

	protected ConcurrentHashMap<Address, List<Nack>> nacks = new ConcurrentHashMap<>();

	private int max_size;

	private Timer t;

	public NACKAcknowledge(MessageProcessor next, int max_size_nack,
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
		Thread send = new Thread("NAck Sender") {
			// int cont = 0;

			@Override
			public void run() {

				while (!stopped) {

					for (NACKAcknowledgeCounter count : counters.values()) {
						boolean first = true;
						for (Message message : count.getSend()) {
							try {
								// if (first) {
								// attachNAcks(message, count.getNACK());
								// first = false;
								// }
								sendNext(message);
								// cont = 0;
							} catch (Exception e) {
								e.printStackTrace();
							}
						}

					}
					// cont++;
					// if (cont >= 100000) {
					// cont = 0;
					synchronized (counters) {
						try {
							counters.wait(1);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					// }
				}

			};
		};
		send.start();
		this.t = new Timer("NAck Timer");
		t.schedule(new TimerTask() {
			@Override
			public void run() {
				for (NACKAcknowledgeCounter count : counters.values()) {
					List<Nack> list = nacks.get(count.to);
					if (list != null) {
						synchronized (list) {
							ArrayList<Message> resend = count.getResend(list);
							// if (resend.size() > 0)
							// System.out.println("Resending " + resend.size());
							for (Message m : resend) {
								// attachNAcks(m, count.getNACK());
								try {
									sendNext(m);
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
							list.clear();
						}
					}
				}
			}
		}, config.retransmit_delay, config.retransmit_delay);

		// Reenvía NACKS
		t.schedule(new TimerTask() {
			@Override
			public void run() {
				for (NACKAcknowledgeCounter count : counters.values()) {
					List<Nack> value = count.getNACK();
					// if (!value.isEmpty())
					// System.out.println("Sending NACKS " + value.size());
					if (value != null)
						synchronized (value) {
							while (!value.isEmpty()) {
								Message ack = Message.newEmptyOutDataMessage(
										MessageType.NACK, count.to);
								attachNAcks(ack, value);
								try {
									sendNext(ack);
								} catch (Exception e1) {
									e1.printStackTrace();
								}
							}
						}
				}
			}
		}, config.nack_delay, config.nack_delay);

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

						NACKAcknowledgeCounter counter = getCounter(m.getFrom());

						if (counter.seqNumberArrived(seq))
							notifyRcvd(Message.deEncapsulate(
									m.getDataAsBytes(), m.getFrom(), m.getTo()));

						Address from = m.getFrom();

						receivedNAckBuffer(from, headerBuffer);

						// Message ackMsg = Message.newEmptyOutDataMessage(
						// MessageType.ACK, m.getFrom());
						// ackMsg.getHeaderBuffer().putInt(seq);
						// sendNext(ackMsg);
					}
				});

		getNext().addMessageListener(MessageType.NACK, new MessageListener() {
			@Override
			public void rcv(Message m, MessageProcessor origin)
					throws Exception {
				receivedNAckBuffer(m.getFrom(), m.getHeaderBuffer());
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
		NACKAcknowledgeCounter c = getCounter(msg.getTo());
		c.send(msg);
		synchronized (counters) {
			counters.notifyAll();
		}
	}

	private NACKAcknowledgeCounter getCounter(Address to) throws Exception {
		NACKAcknowledgeCounter counter = counters.get(to);
		if (counter == null) {
			synchronized (counters) {
				counter = counters.get(to);
				if (counter == null) {
					counter = new NACKAcknowledgeCounter(to, max_size_nack,
							nack_delay, ack_delay, config);
					counters.put(to, counter);
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
		counters.remove(addr);
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}

	private void receivedNAckBuffer(Address from, ByteBuffer headerBuffer)
			throws Exception {
		if (headerBuffer.hasRemaining()) {
			int acksCount = headerBuffer.getInt();
			for (int i = 0; i < acksCount; i++) {

				int seq = headerBuffer.getInt();
				boolean lastReceived = headerBuffer.getBoolean();

				List<Nack> list = nacks.get(from);
				if (list == null) {
					synchronized (nacks) {
						list = nacks.get(from);
						if (list == null) {
							list = new ArrayList<>();
							nacks.put(from, list);
						}
					}
				}

				synchronized (list) {
					list.add(new Nack(seq, lastReceived));
				}

				if (lastReceived) {
					NACKAcknowledgeCounter c = getCounter(from);
					c.confirm(seq);
					synchronized (counters) {
						counters.notifyAll();
					}
				}
			}

		}
	}

	private void attachNAcks(Message msg, List<Nack> list) {
		if (list == null)
			return;
		ByteBuffer buff = msg.getHeaderBuffer();
		int size = msg.getSize();
		int diff = (max_size - 4) - size;
		int count = diff / 16;
		if (count > 0)
			synchronized (list) {
				if (!list.isEmpty()) {
					buff.putInt(Math.min(count, list.size()));
					Iterator<Nack> it = list.iterator();
					for (int i = 0; i < count && it.hasNext(); i++) {
						Nack n = it.next();
						buff.putInt(n.seq);
						buff.putBoolean(n.last);
						it.remove();
					}

				}
			}
	}

}
