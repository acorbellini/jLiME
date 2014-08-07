package edu.jlime.rpc.fd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.PeerJlime;
import edu.jlime.rpc.message.Address;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.StackElement;

public class PingFailureDetection implements StackElement, FailureProvider {

	private boolean stopped = false;

	private Logger log = Logger.getLogger(PingFailureDetection.class);

	private List<FailureListener> list = new ArrayList<>();

	private int ping_delay = 5000;

	private int max_missed = 20;

	private MessageProcessor conn;

	private ConcurrentHashMap<Address, Integer> tries = new ConcurrentHashMap<>();

	private ConcurrentHashMap<Address, PeerJlime> peers = new ConcurrentHashMap<>();

	@Override
	public void addFailureListener(FailureListener l) {
		list.add(l);
	}

	public PingFailureDetection(final MessageProcessor conn) {
		this.conn = conn;
		addPingProvider(conn);
		// conn.addMessageListener(MessageType.PING, new DEFMessageListener() {
		// @Override
		// public void rcv(DEFMessage defMessage, MessageProcessor origin)
		// throws Exception {
		// DEFMessage msg = DEFMessage.newEmptyOutDataMessage(
		// MessageType.PONG, defMessage.getFrom());
		// conn.queue(msg);
		// }
		// });
		// conn.addMessageListener(MessageType.PONG, new DEFMessageListener() {
		// @Override
		// public void rcv(DEFMessage defMessage, MessageProcessor origin)
		// throws Exception {
		// pongArrived(defMessage);
		// }
		// });

	}

	@Override
	public void addPeerToMonitor(PeerJlime peer) throws Exception {
		tries.put(peer.getAddr(), 0);
		peers.put(peer.getAddr(), peer);
	}

	@Override
	public void cleanupOnFailedPeer(Address peer) {
	}

	@Override
	public void start() throws Exception {
		Thread t = new Thread("Pinger Thread") {
			public void run() {
				while (!stopped) {
					try {
						Thread.sleep(ping_delay);
					} catch (InterruptedException excep) {
						excep.printStackTrace();
					}
					for (Entry<Address, Integer> e : new ArrayList<>(
							tries.entrySet())) {
						try {
							if (log.isDebugEnabled())
								log.debug("Sending ping to " + e.getKey()
										+ ", try number "
										+ tries.get(e.getKey()));
							conn.queue(Message.newEmptyOutDataMessage(
									MessageType.PING, e.getKey()));
						} catch (Exception excep) {
							excep.printStackTrace();
						}
						tries.put(e.getKey(), e.getValue() + 1);
					}
				}
			};
		};
		t.start();

		Thread failure = new Thread("Failure Detect Thread") {
			public void run() {
				while (!stopped) {
					for (Entry<Address, Integer> e : new ArrayList<>(
							tries.entrySet())) {
						if (e.getValue() >= max_missed) {
							tries.remove(e.getKey());
							PeerJlime peerThatFailed = peers.remove(e.getKey());
							if (peerThatFailed != null)
								for (FailureListener l : list)
									l.nodeFailed(peerThatFailed);

						}
					}
					try {
						Thread.sleep(ping_delay);
					} catch (InterruptedException excep) {
						excep.printStackTrace();
					}
				}
			};
		};
		failure.start();
	}

	public void addPingProvider(MessageProcessor conn) {
		conn.addAllMessageListener(new MessageListener() {
			@Override
			public void rcv(Message m, MessageProcessor origin)
					throws Exception {
				pongArrived(m);
			}
		});
	}

	@Override
	public void stop() throws Exception {
		this.stopped = true;
	}

	private void pongArrived(Message m) {
		if (log.isDebugEnabled())
			log.debug("Received pong from " + m.getFrom() + ".");
		tries.put(m.getFrom(), 0);
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}

}
