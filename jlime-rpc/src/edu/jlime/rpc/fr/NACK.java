package edu.jlime.rpc.fr;

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

public class NACK extends SimpleMessageProcessor {

	public static final int HEADER = Header.HEADER + 4;

	Object[] locks = new Object[1021];

	Logger log = Logger.getLogger(NACK.class);

	ConcurrentHashMap<Address, NACKCounter> counters = new ConcurrentHashMap<>();

	CopyOnWriteArrayList<NACKCounter> counterList = new CopyOnWriteArrayList<>();

	private int max_size_nack;

	private int nack_delay;

	private int ack_delay;

	private Configuration config;

	// protected ConcurrentHashMap<Address, HashSet<Integer>> acks = new
	// ConcurrentHashMap<>();

	int max_size;

	private Timer t;

	public NACK(MessageProcessor next, int max_size_nack, int nack_delay,
			int ack_delay, int max_size, Configuration config) {
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
		this.t = new Timer("Ack Timer");

		t.schedule(new TimerTask() {
			@Override
			public void run() {
				for (NACKCounter count : counterList) {
					try {
						count.sendNextExpectedNumber();
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}
		}, config.retransmit_delay, config.retransmit_delay);

		t.schedule(new TimerTask() {

			@Override
			public void run() {
				for (NACKCounter count : counterList) {
					try {
						count.sendAcks();
					} catch (Exception e) {
						e.printStackTrace();
					}
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
						int confirmed = headerBuffer.getInt();
						if (log.isTraceEnabled())
							log.trace("Received Ack'd msg with seq # " + seq
									+ " from " + m.getFrom());

						NACKCounter counter = getCounter(m.getFrom());
						if (counter != null) {
							if (counter.seqNumberArrived(seq)) {
								notifyRcvd(Message.deEncapsulate(
										m.getDataBuffer(), m.getFrom(),
										m.getTo()));
							}
							counter.sync(confirmed);

							// counter.receivedAckBuffer(headerBuffer);
						}
					}
				});

		getNext().addMessageListener(MessageType.ACK, new MessageListener() {
			@Override
			public void rcv(Message m, MessageProcessor origin)
					throws Exception {
				NACKCounter counter = getCounter(m.getFrom());
				if (counter != null)
					counter.receivedAckBuffer(m.getHeaderBuffer());
			}
		});

		getNext().addMessageListener(MessageType.SYN, new MessageListener() {
			@Override
			public void rcv(Message m, MessageProcessor origin)
					throws Exception {
				NACKCounter counter = getCounter(m.getFrom());
				if (counter != null)
					counter.sync(m.getHeaderBuffer().getInt());
			}
		});
	}

	@Override
	public void send(Message msg) throws Exception {
		NACKCounter c = getCounter(msg.getTo());

		c.send(msg);
		synchronized (counters) {
			counters.notifyAll();
		}
	}

	private NACKCounter getCounter(Address to) throws Exception {
		NACKCounter counter = counters.get(to);
		if (counter == null) {
			synchronized (counters) {
				counter = counters.get(to);
				if (counter == null) {
					counter = new NACKCounter(this, to, max_size_nack,
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
		NACKCounter count = counters.remove(addr);
		if (count != null)
			counterList.remove(count);
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}

}
