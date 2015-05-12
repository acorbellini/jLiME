package edu.jlime.rpc.fd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.FailureListener;
import edu.jlime.core.transport.FailureProvider;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.NetworkConfiguration;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.StackElement;

public class PingFailureDetection implements StackElement, FailureProvider {

	private boolean stopped = false;

	private Logger log = Logger.getLogger(PingFailureDetection.class);

	private List<FailureListener> list = new ArrayList<>();

	private int ping_delay;

	private int max_missed;

	private MessageProcessor conn;

	private ExecutorService failure = Executors.newCachedThreadPool();

	private ConcurrentHashMap<Address, AtomicInteger> tries = new ConcurrentHashMap<>();

	@Override
	public void addListener(FailureListener l) {
		list.add(l);
	}

	public PingFailureDetection(final MessageProcessor conn,
			NetworkConfiguration config) {
		this.conn = conn;
		this.max_missed = config.max_pings;
		this.ping_delay = config.ping_delay;
		addPingProvider(conn);
	}

	@Override
	public void addPeerToMonitor(Peer peer) throws Exception {
		tries.put(peer.getAddress(), new AtomicInteger(0));
	}

	@Override
	public void start() throws Exception {
		Thread t = new Thread("Pinger Thread") {
			public void run() {
				while (!stopped) {
					try {
						Thread.sleep(ping_delay);

						for (Entry<Address, AtomicInteger> e : tries.entrySet()) {
							AtomicInteger current = e.getValue();
							Address peer = e.getKey();
							if (current.get() > max_missed)
								failed(peer, current.get());
							else {
								if (log.isDebugEnabled())
									log.debug("Sending ping to " + peer
											+ ", try number " + current.get());
								try {
									conn.send(Message.newEmptyOutDataMessage(
											MessageType.PING, peer));
								} catch (Exception e1) {
									e1.printStackTrace();
								}
								current.incrementAndGet();
							}

						}
					} catch (InterruptedException excep) {
						excep.printStackTrace();
					}
				}
			};
		};
		t.start();
	}

	protected void failed(final Address addr, Integer t) {
		if (log.isDebugEnabled())
			log.debug("Removing " + addr + " tried " + t + " times.");

		tries.remove(addr);

		failure.execute(new Runnable() {

			@Override
			public void run() {
				for (FailureListener l : list)
					l.nodeFailed(addr);
			}
		});

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
		// if (log.isDebugEnabled())
		// log.debug("Received pong from " + m.getFrom() + ".");
		AtomicInteger count = tries.get(m.getFrom());
		if (count != null)
			count.set(0);
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}

	@Override
	public void cleanupOnFailedPeer(Address address) {
		// TODO Auto-generated method stub

	}

}
