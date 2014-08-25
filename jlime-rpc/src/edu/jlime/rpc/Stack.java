package edu.jlime.rpc;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.DiscoveryProvider;
import edu.jlime.core.transport.FailureProvider;
import edu.jlime.core.transport.Streamer;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.data.DataProcessor;
import edu.jlime.rpc.data.DataProvider;
import edu.jlime.rpc.discovery.MultiCastDiscovery;
import edu.jlime.rpc.fd.PingFailureDetection;
import edu.jlime.rpc.fr.Acknowledge;
import edu.jlime.rpc.frag.Fragmenter;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.StackElement;
import edu.jlime.rpc.multi.MultiInterface;
import edu.jlime.rpc.np.NetworkProtocol;
import edu.jlime.util.NetworkUtils;
import edu.jlime.util.RingQueue;

public class Stack {

	private LinkedList<StackElement> stackElements = new LinkedList<>();

	private FailureProvider fail;

	private DiscoveryProvider disco;

	private DataProcessor data;

	private Streamer streamer;

	public void start() throws Exception {
		for (StackElement m : stackElements) {
			m.start();
		}
	}

	public void addProc(StackElement p) {
		stackElements.add(p);
	}

	public void cleanup(Address address) {
		for (ListIterator<StackElement> iterator = stackElements
				.listIterator(stackElements.size()); iterator.hasPrevious();) {
			StackElement listElement = iterator.previous();
			listElement.cleanupOnFailedPeer(address);
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

	private void setDisco(DiscoveryProvider disco) {
		this.disco = disco;
	}

	private void setFD(FailureProvider fail) {
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

	public static Stack tcpStack(Configuration config, Address local,
			String name) {

		String iface = NetworkUtils.getFirstHostAddress();

		NetworkProtocol udp = NetworkProtocolFactory.udp(local, config)
				.getProtocol(iface);

		NetworkProtocol tcp = NetworkProtocolFactory.tcp(local, config)
				.getProtocol(iface);

		NetworkProtocol mcast = NetworkProtocolFactory.mcast(local, config)
				.getProtocol(iface);

		final DataProcessor data = new DataProcessor(tcp);

		MultiCastDiscovery disco = new MultiCastDiscovery(local, name, config,
				mcast, udp);
		disco.addAddressListProvider(tcp);
		disco.addAddressListProvider(udp);

		PingFailureDetection fail = new PingFailureDetection(udp);
		fail.addPingProvider(tcp);

		Stack tcpStack = Stack.newStack(tcp, udp, mcast, data, disco, fail);
		tcpStack.setFD(fail);
		tcpStack.setDisco(disco);
		tcpStack.setData(data);
		tcpStack.setStreamer(tcp);
		return tcpStack;
	}

	public static Stack udpStack(Configuration config, Address id, String name) {

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
		Acknowledge ack = new Acknowledge(udp, config.max_msg_size,
				config.nack_delay, config.ack_delay);

		Fragmenter frag = new Fragmenter(ack, config.max_msg_size);

		DataProcessor data = new DataProcessor(frag);

		MultiDiscovery disco = new MultiDiscovery();

		MultiCastDiscovery mcastDisco = new MultiCastDiscovery(id, name,
				config, mcast, udp);
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

	public void setMetrics(Metrics metrics) {
		for (StackElement se : stackElements) {
			se.setMetrics(metrics);
		}
	}
}