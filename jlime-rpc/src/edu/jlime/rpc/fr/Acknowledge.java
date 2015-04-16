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

public class Acknowledge extends SimpleMessageProcessor {

	public static final int HEADER = Header.HEADER + 4;

	Logger log = Logger.getLogger(Acknowledge.class);

	ConcurrentHashMap<Address, AcknowledgeCounter> counters = new ConcurrentHashMap<>();

	CopyOnWriteArrayList<AcknowledgeCounter> counterList = new CopyOnWriteArrayList<>();

	private Configuration config;

	int max_size;

	private Timer t;

	public Acknowledge(MessageProcessor next, int max_size, Configuration config) {
		super(next, "Acknowledge");
		this.config = config;
		this.max_size = max_size;
	}

	@Override
	public void onStart() throws Exception {
		this.t = new Timer("Ack Timer");

		t.schedule(new TimerTask() {
			@Override
			public void run() {
				for (AcknowledgeCounter count : counterList) {
					count.resend();
				}
			}
		}, config.retransmit_delay, config.retransmit_delay);

		t.schedule(new TimerTask() {

			@Override
			public void run() {
				for (AcknowledgeCounter count : counterList) {
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
						if (log.isTraceEnabled())
							log.trace("Received Ack'd msg with seq # " + seq
									+ " from " + m.getFrom());

						AcknowledgeCounter counter = getCounter(m.getFrom());
						if (counter != null) {
							if (counter.seqNumberArrived(seq)) {
								notifyRcvd(Message.deEncapsulate(
										m.getDataBuffer(), m.getFrom(),
										m.getTo()));
							}
							counter.receivedAckBuffer(headerBuffer);
						}
					}
				});

		getNext().addMessageListener(MessageType.ACK, new MessageListener() {
			@Override
			public void rcv(Message m, MessageProcessor origin)
					throws Exception {
				AcknowledgeCounter counter = getCounter(m.getFrom());
				if (counter != null)
					counter.receivedAckBuffer(m.getHeaderBuffer());
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
					counter = new AcknowledgeCounter(this, to, config);
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

}
