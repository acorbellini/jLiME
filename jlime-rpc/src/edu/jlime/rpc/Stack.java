package edu.jlime.rpc;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.data.DataProcessor;
import edu.jlime.rpc.data.DataProvider;
import edu.jlime.rpc.discovery.DiscoveryProvider;
import edu.jlime.rpc.discovery.MultiCastDiscovery;
import edu.jlime.rpc.fd.FailureProvider;
import edu.jlime.rpc.fd.PingFailureDetection;
import edu.jlime.rpc.fr.Acknowledge;
import edu.jlime.rpc.frag.Fragmenter;
import edu.jlime.rpc.message.Address;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.StackElement;
import edu.jlime.rpc.multi.MultiInterface;
import edu.jlime.rpc.np.NetworkProtocol;
import edu.jlime.rpc.np.Streamer;
import edu.jlime.util.NetworkUtils;
import edu.jlime.util.RingQueue;

public class Stack {

	private LinkedList<StackElement> stackElements = new LinkedList<>();

	private FailureProvider fail;

	private DiscoveryProvider disco;

	private DataProcessor data;

	private Streamer streamer;

	private HashMap<String, Bus> buses = new HashMap<>();

	public void start() throws Exception {
		for (StackElement m : stackElements) {
			m.start();
		}
	}

	public void addProc(StackElement p) {
		stackElements.add(p);
	}

	public void cleanupOnFailedPeer(Address peer) {
		for (ListIterator<StackElement> iterator = stackElements
				.listIterator(stackElements.size()); iterator.hasPrevious();) {
			StackElement listElement = iterator.previous();
			listElement.cleanupOnFailedPeer(peer);
		}
	}

	public void stop() throws Exception {
		for (StackElement m : stackElements) {
			m.stop();
		}
	}

	public static Stack newStack(StackElement... elements) {
		Stack ret = new Stack();
		for (StackElement defStackElement : elements)
			ret.addProc(defStackElement);

		return ret;
	}

	private void setStreamer(Streamer s) {
		this.streamer = s;
	}

	private void setData(DataProcessor data) {
		this.data = data;
	}

	private void setDisco(MultiDiscovery disco) {
		this.disco = disco;
	}

	private void setFD(PingFailureDetection fail) {
		this.fail = fail;
	}

	public DataProvider getData() {
		return data;
	}

	public Streamer getStreamer() {
		return streamer;
	}

	public DiscoveryProvider getDiscovery() {
		return disco;
	}

	public FailureProvider getFailureDetection() {
		return fail;
	}

	public static Stack tcpStack(Configuration config, UUID localID) {

		String iface = NetworkUtils.getFirstHostAddress();

		NetworkProtocol udp = NetworkProtocolFactory.udp(localID, config)
				.getProtocol(iface);

		NetworkProtocol tcp = NetworkProtocolFactory.tcp(localID, config)
				.getProtocol(iface);

		NetworkProtocol mcast = NetworkProtocolFactory.mcast(localID, config)
				.getProtocol(iface);

		// NetworkProtocolFactory udpFactory =
		// NetworkProtocolFactory.udp(localID,
		// config);
		//
		// NetworkProtocolFactory tcpFactory =
		// NetworkProtocolFactory.tcp(localID,
		// config);
		//
		// NetworkProtocolFactory mcastFactory = NetworkProtocolFactory.mcast(
		// localID, config);
		//
		// MultiInterface udp = MultiInterface.create(AddressType.UDP, config,
		// udpFactory);
		//
		// MultiInterface tcp = MultiInterface.create(AddressType.TCP, config,
		// tcpFactory);
		//
		// MultiInterface mcast = MultiInterface.create(AddressType.MCAST,
		// config,
		// mcastFactory);

		final DataProcessor data = new DataProcessor(tcp);

		MultiDiscovery disco = new MultiDiscovery();

		MultiCastDiscovery mcastDisco = new MultiCastDiscovery(localID, config,
				mcast, udp);
		mcastDisco.addAddressListProvider(tcp);
		mcastDisco.addAddressListProvider(udp);
		// mcastDisco.setAddressTester(new AddressTester() {
		//
		// @Override
		// public boolean test(UUID id, DEFSocketAddress defSocketAddress) {
		// tcp.send(DEFMessage
		// .encapsulate(msg, MessageType.TEST, from, to));
		// DEFByteBuffer buff = new DEFByteBuffer();
		// buff.putUUID(id);
		// try {
		// byte[] resp = data.sendData(buff.build(), defSocketAddress,
		// true);
		// return false;
		// } catch (Exception e) {
		// return false;
		// }
		// }
		// });
		disco.addDisco(mcastDisco);

		PingFailureDetection fail = new PingFailureDetection(udp);
		fail.addPingProvider(tcp);

		Stack tcpStack = Stack.newStack(tcp, udp, mcast, data, disco, fail);
		tcpStack.setFD(fail);
		tcpStack.setDisco(disco);
		tcpStack.setData(data);
		tcpStack.setStreamer(tcp);
		return tcpStack;
	}

	public static Stack udpStack(Configuration config, UUID id) {

		NetworkProtocolFactory udpFactory = NetworkProtocolFactory.udp(id,
				config);

		NetworkProtocolFactory tcpFactory = NetworkProtocolFactory.tcp(id,
				config);

		NetworkProtocolFactory mcastFactory = NetworkProtocolFactory.mcast(id,
				config);

		MultiInterface udp = MultiInterface.create(AddressType.UDP, config,
				udpFactory);

		MultiInterface tcp = MultiInterface.create(AddressType.TCP, config,
				tcpFactory);

		MultiInterface mcast = MultiInterface.create(AddressType.MCAST, config,
				mcastFactory);

		// DEFMessageBundler bundler = new DEFMessageBundler(udp,
		// config.max_msg_size);

		Acknowledge ack = new Acknowledge(udp, config.max_msg_size,
				config.nack_delay, config.ack_delay);

		Fragmenter frag = new Fragmenter(ack, config.max_msg_size);

		DataProcessor data = new DataProcessor(frag);

		MultiDiscovery disco = new MultiDiscovery();

		MultiCastDiscovery mcastDisco = new MultiCastDiscovery(id, config,
				mcast, udp);
		mcastDisco.addAddressListProvider(udp);
		mcastDisco.addAddressListProvider(tcp);
		disco.addDisco(mcastDisco);

		PingFailureDetection fail = new PingFailureDetection(udp);
		fail.addPingProvider(tcp);

		Stack tcpStack = Stack.newStack(tcp, udp,
		// bundler,
				ack, frag, mcast, data, disco, fail);
		tcpStack.setFD(fail);
		tcpStack.setDisco(disco);
		tcpStack.setData(data);
		tcpStack.setStreamer(tcp);
		return tcpStack;
	}

	public interface BusAction extends Runnable {

		public void run();
	}

	public static class Bus {

		private ExecutorService exec = Executors
				.newCachedThreadPool(new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory()
								.newThread(r);
						t.setName("Bus Thread Pool");
						return t;
					}
				});

		private volatile boolean stopped = false;

		private RingQueue in = new RingQueue();

		private RingQueue out = new RingQueue();

		public Bus() {
			Thread tIn = new Thread("Bus in") {
				public void run() {
					while (!stopped) {
						Object[] els = in.get();
						if (stopped)
							return;
						for (Object object : els) {
							out.add(object);
						}
					}
				};
			};

			Thread tOut = new Thread("Bus out") {
				public void run() {
					while (!stopped) {
						Object[] els = out.get();
						if (stopped)
							return;
						for (Object object : els) {
							BusAction buse = (BusAction) object;
							exec.execute(buse);
						}
					}
				};
			};
			tIn.start();
			tOut.start();
		}

		public void stop() {
			stopped = true;
			in.add(new Object());
			out.add(new Object());
		}

		public void add(BusAction act) {
			in.add(act);
		}

	}

	public void add(String queue, BusAction act) {
		// queue = "ONLY";
		Bus b = buses.get(queue);
		if (b == null) {
			synchronized (this) {
				b = buses.get(queue);
				if (b == null) {
					b = new Bus();
					buses.put(queue, b);
				}
			}
		}
		b.add(act);
	}

	public void setMetrics(Metrics metrics) {
		for (StackElement se : stackElements) {
			se.setMetrics(metrics);
		}
	}
}