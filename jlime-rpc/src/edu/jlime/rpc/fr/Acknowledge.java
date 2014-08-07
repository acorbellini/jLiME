package edu.jlime.rpc.fr;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Address;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;

public class Acknowledge extends SimpleMessageProcessor {

	Logger log = Logger.getLogger(Acknowledge.class);

	ConcurrentHashMap<Address, AcknowledgeCounter> counters = new ConcurrentHashMap<>();

	private int max_size_nack;

	private int nack_delay;

	private int ack_delay;

	public Acknowledge(MessageProcessor next, int max_size_nack,
			int nack_delay, int ack_delay) {
		super(next, "DEFAck");
		this.max_size_nack = max_size_nack;
		this.nack_delay = nack_delay;
		this.ack_delay = ack_delay;
	}

	@Override
	public void start() throws Exception {
		Thread sender = new Thread("DEF Ack Sender And Resender") {
			public void run() {
				while (!stopped) {
					for (AcknowledgeCounter count : counters.values()) {
						for (Message defMessage : count.getSend()) {
							try {
								sendNext(defMessage);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
					synchronized (counters) {
						try {
							counters.wait(1);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}

				}
			};
		};
		sender.start();

		getNext().addMessageListener(MessageType.ACK_SEQ,
				new MessageListener() {
					@Override
					public void rcv(Message m, MessageProcessor origin)
							throws Exception {
						int seq = m.getHeaderBuffer().getInt();
						if (log.isTraceEnabled())
							log.trace("Received Ack'd msg with seq # " + seq
									+ " from " + m.getFrom());
						AcknowledgeCounter counter = getCounter(m.getFrom());

						if (counter.seqNumberArrived(seq))
							notifyRcvd(Message.deEncapsulate(
									m.getDataAsBytes(), m.getFrom(), m.getTo()));

						Message ackMsg = Message.newEmptyOutDataMessage(
								MessageType.ACK, m.getFrom());
						ackMsg.getHeaderBuffer().putInt(seq);
						sendNext(ackMsg);
					}
				});

		// getNext().addMessageListener(MessageType.NACK,
		// new DEFMessageListener() {
		//
		// @Override
		// public void rcv(DEFMessage m, MessageProcessor origin)
		// throws Exception {
		// int missingNumber = m.getHeaderReader().getInt();
		// TIntArrayList missing = new TIntArrayList();
		// DEFByteBufferReader reader = m.getDataReader();
		// for (int i = 0; i < missingNumber; i++) {
		// missing.add(reader.getInt());
		// }
		// if (log.isDebugEnabled())
		// log.debug("Received NACK with " + missing
		// + " messages lost from " + m.getFrom());
		// DEFAckCountPerNode c = getCounter(m.getFrom());
		// c.resend(missing);
		// }
		// });

		// getNext().addMessageListener(MessageType.ACK, new
		// DEFMessageListener() {
		//
		// @Override
		// public void rcv(DEFMessage m, MessageProcessor origin)
		// throws Exception {
		// int ackConfirm = m.getHeaderReader().getInt();
		// if (log.isDebugEnabled())
		// log.debug("Received ACK asking to get minimum seq number received with expected "
		// + ackConfirm);
		// DEFAckCountPerNode c = getCounter(m.getFrom());
		// c.getSender().check(ackConfirm);
		// }
		// });

		getNext().addMessageListener(MessageType.ACK, new MessageListener() {
			@Override
			public void rcv(Message m, MessageProcessor origin)
					throws Exception {
				int ackConfirm = m.getHeaderBuffer().getInt();
				if (log.isTraceEnabled())
					log.trace("Received message from " + m.getFrom()
							+ " that confirms " + ackConfirm);
				AcknowledgeCounter c = getCounter(m.getFrom());
				c.confirm(ackConfirm);
				synchronized (counters) {
					counters.notifyAll();
				}
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
					counter = new AcknowledgeCounter(to, max_size_nack,
							nack_delay, ack_delay);
					counters.put(to, counter);
				}
			}
		}
		return counter;

	}

	@Override
	public void onStop() throws Exception {
	}

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		counters.remove(addr);
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}

}
