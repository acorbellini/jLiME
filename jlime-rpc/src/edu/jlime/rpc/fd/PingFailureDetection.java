package edu.jlime.rpc.fd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.FailureListener;
import edu.jlime.core.transport.FailureProvider;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.Configuration;
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

	private ConcurrentHashMap<Address, Integer> tries = new ConcurrentHashMap<>();

	@Override
	public void addListener(FailureListener l) {
		list.add(l);
	}

	public PingFailureDetection(final MessageProcessor conn,
			Configuration config) {
		this.conn = conn;
		this.max_missed = config.max_pings;
		this.ping_delay = config.ping_delay;
		addPingProvider(conn);
	}

	@Override
	public void addPeerToMonitor(Peer peer) throws Exception {
		tries.put((Address) peer.getAddress(), 0);
	}

	@Override
	public void start() throws Exception {
		Thread t = new Thread("Pinger Thread") {
			public void run() {
				while (!stopped) {
					try {
						Thread.sleep(ping_delay);

						ArrayList<Entry<Address, Integer>> arrayList = null;
						synchronized (tries) {
							arrayList = new ArrayList<>(tries.entrySet());
						}

						for (Entry<Address, Integer> e : arrayList) {
							if (e.getValue() > max_missed)
								failed(e.getKey(), e.getValue());
							else {
								// if (log.isDebugEnabled())
								Integer integer = tries.get(e.getKey());
								if (integer != null && integer != 0)
									log.info("Sending ping to " + e.getKey()
											+ ", try number " + integer);
								conn.queue(Message.newEmptyOutDataMessage(
										MessageType.PING, e.getKey()));
								tries.put(e.getKey(), e.getValue() + 1);

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
		if (log.isDebugEnabled())
			log.debug("Received pong from " + m.getFrom() + ".");
		if (tries.containsKey(m.getFrom()))
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
