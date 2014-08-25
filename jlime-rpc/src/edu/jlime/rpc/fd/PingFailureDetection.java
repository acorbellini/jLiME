package edu.jlime.rpc.fd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.FailureListener;
import edu.jlime.core.transport.FailureProvider;
import edu.jlime.metrics.metric.Metrics;
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

	private ConcurrentHashMap<Address, Peer> peers = new ConcurrentHashMap<>();

	@Override
	public void addListener(FailureListener l) {
		list.add(l);
	}

	public PingFailureDetection(final MessageProcessor conn) {
		this.conn = conn;
		addPingProvider(conn);
	}

	@Override
	public void addPeerToMonitor(Peer peer) throws Exception {
		tries.put((Address) peer.getAddress(), 0);
		peers.put((Address) peer.getAddress(), peer);
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
					for (Entry<Address, Peer> e : new ArrayList<>(
							peers.entrySet())) {
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
						tries.put(e.getKey(), tries.get(e.getKey()) + 1);
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
							Peer peerThatFailed = peers.remove(e.getKey());
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

	@Override
	public void cleanupOnFailedPeer(Address address) {
		// TODO Auto-generated method stub

	}

}
