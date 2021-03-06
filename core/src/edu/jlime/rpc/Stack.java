package edu.jlime.rpc;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

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
import edu.jlime.rpc.fr.NACK;
import edu.jlime.rpc.frag.Fragmenter;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.StackElement;
import edu.jlime.rpc.np.NetworkProtocol;
import edu.jlime.rpc.rabbit.RabbitProcessor;
import edu.jlime.rpc.udp.UDP;
import edu.jlime.rpc.zeromq.ZeroMQProcessor;
import edu.jlime.util.NetworkUtils;

public class Stack implements Iterable<StackElement> {

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
		for (ListIterator<StackElement> iterator = stackElements.listIterator(stackElements.size()); iterator
				.hasPrevious();) {
			StackElement listElement = iterator.previous();
			listElement.cleanupOnFailedPeer(address);
		}
	}

	public void stop() throws Exception {
		for (StackElement m : stackElements) {
			m.stop();
		}

		stackElements.clear();

	}

	public static Stack newStack(StackElement... elements) {
		Stack ret = new Stack();
		for (StackElement stackElement : elements)
			ret.addProc(stackElement);

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

	public static Stack tcpStack(NetworkConfiguration config, Address local, String name) {

		String iface = NetworkUtils.getFirstHostAddress();

		NetworkProtocol udp = NetworkProtocolFactory.udp(local, config).getProtocol(iface);

		NetworkProtocol tcp = NetworkProtocolFactory.tcp(local, config).getProtocol(iface);

		NetworkProtocol mcast = NetworkProtocolFactory.mcast(local, config).getProtocol(iface);

		final DataProcessor data = new DataProcessor(tcp, config);

		MultiCastDiscovery disco = new MultiCastDiscovery(local, name, config, mcast, udp);
		disco.addAddressListProvider(tcp);
		disco.addAddressListProvider(udp);

		PingFailureDetection fail = new PingFailureDetection(udp, config);
		fail.addPingProvider(tcp);

		Stack tcpStack = Stack.newStack(tcp, udp, mcast, data, disco, fail);
		tcpStack.setFD(fail);
		tcpStack.setDisco(disco);
		tcpStack.setData(data);
		tcpStack.setStreamer(tcp);
		return tcpStack;
	}

	public static Stack udpStack(NetworkConfiguration config, Address local, String name) {

		String iface = NetworkUtils.getFirstHostAddress();

		NetworkProtocol udp = NetworkProtocolFactory.udp(local, config).getProtocol(iface);

		NetworkProtocol mcast = NetworkProtocolFactory.mcast(local, config).getProtocol(iface);

		MessageProcessor ack = null;
		Fragmenter frag = null;
		if (config.useNACK) {

			int max_size = config.max_msg_size - UDP.HEADER - NACK.HEADER;

			ack = new NACK(udp, max_size, config);

			frag = new Fragmenter(ack, max_size);
		} else {

			int max_size = config.max_msg_size - UDP.HEADER - Acknowledge.HEADER;

			ack = new Acknowledge(udp, max_size, config);

			frag = new Fragmenter(ack, max_size);
		}
		DataProcessor data = new DataProcessor(frag, config);

		MultiCastDiscovery disco = new MultiCastDiscovery(local, name, config, mcast, udp);
		disco.addAddressListProvider(udp);

		PingFailureDetection fail = new PingFailureDetection(udp, config);
		Stack tcpStack = Stack.newStack(udp, ack, frag, mcast, data, disco, fail);
		tcpStack.setFD(fail);
		tcpStack.setDisco(disco);
		tcpStack.setData(data);
		return tcpStack;
	}

	public static Stack tcpNioStack(NetworkConfiguration config, Address local, String name) {

		String iface = NetworkUtils.getFirstHostAddress();

		NetworkProtocol udp = NetworkProtocolFactory.udp(local, config).getProtocol(iface);

		TCPNIO tcp = new TCPNIO(local, config, iface);

		NetworkProtocol mcast = NetworkProtocolFactory.mcast(local, config).getProtocol(iface);

		Fragmenter frag = new Fragmenter(tcp, config.tcpnio_max_msg_size - TCPNIO.HEADER);

		DataProcessor data = new DataProcessor(frag, config);

		MultiCastDiscovery disco = new MultiCastDiscovery(local, name, config, mcast, udp);
		disco.addAddressListProvider(udp);
		disco.addAddressListProvider(tcp);

		PingFailureDetection fail = new PingFailureDetection(udp, config);
		fail.addPingProvider(tcp);

		Stack tcpStack = Stack.newStack(tcp, udp, frag, mcast, data, disco, fail);
		tcpStack.setFD(fail);
		tcpStack.setDisco(disco);
		tcpStack.setData(data);
		// tcpStack.setStreamer(tcp);
		return tcpStack;
	}

	public static Stack udpNioStack(NetworkConfiguration config, Address local, String name) {

		String iface = NetworkUtils.getFirstHostAddress(true);

		UDPNIO udp = new UDPNIO(local, config, iface);

		NetworkProtocol mcast = NetworkProtocolFactory.mcast(local, config).getProtocol(iface);

		MessageProcessor ack = null;
		Fragmenter frag = null;
		if (config.useNACK) {

			int max_size = config.max_msg_size - UDPNIO.HEADER - NACK.HEADER;

			ack = new NACK(udp, max_size, config);

			frag = new Fragmenter(ack, max_size);
		} else {

			int max_size = config.max_msg_size - UDPNIO.HEADER - Acknowledge.HEADER;

			ack = new Acknowledge(udp, max_size, config);

			frag = new Fragmenter(ack, max_size);
		}

		// int max_size = config.max_msg_size - UDPNIO.HEADER -
		// UDPResender.HEADER;
		//
		// UDPResender ack = new UDPResender(udp, config, max_size);
		//
		// Fragmenter frag = new Fragmenter(ack, max_size);

		DataProcessor data = new DataProcessor(frag, config);

		MultiCastDiscovery disco = new MultiCastDiscovery(local, name, config, mcast, udp);
		disco.addAddressListProvider(udp);

		PingFailureDetection fail = new PingFailureDetection(udp, config);

		Stack tcpStack = Stack.newStack(udp, ack, frag, mcast, data, disco, fail);
		tcpStack.setFD(fail);
		tcpStack.setDisco(disco);
		tcpStack.setData(data);
		return tcpStack;
	}

	public static Stack zeroMqStack(NetworkConfiguration config, Address local, String name) {

		String iface = NetworkUtils.getFirstHostAddress(true);

		ZeroMQProcessor zmq = new ZeroMQProcessor(config, iface, local);

		NetworkProtocol mcast = NetworkProtocolFactory.mcast(local, config).getProtocol(iface);

		DataProcessor data = new DataProcessor(zmq, config);

		MultiCastDiscovery disco = new MultiCastDiscovery(local, name, config, mcast, zmq);
		disco.addAddressListProvider(zmq);

		PingFailureDetection fail = new PingFailureDetection(zmq, config);

		Stack tcpStack = Stack.newStack(zmq, mcast, data, disco, fail);
		tcpStack.setFD(fail);
		tcpStack.setDisco(disco);
		tcpStack.setData(data);
		return tcpStack;
	}

	public static Stack jnetStack(NetworkConfiguration config, Address local, String name) {

		String iface = NetworkUtils.getFirstHostAddress();

		NetworkProtocol udp = NetworkProtocolFactory.udp(local, config).getProtocol(iface);

		JNET tcp = new JNET(local, config, iface);

		NetworkProtocol mcast = NetworkProtocolFactory.mcast(local, config).getProtocol(iface);
		//
		Fragmenter frag = new Fragmenter(tcp, config.max_msg_size - TCPNIO.HEADER);

		DataProcessor data = new DataProcessor(frag, config);

		MultiCastDiscovery disco = new MultiCastDiscovery(local, name, config, mcast, udp);
		disco.addAddressListProvider(udp);
		disco.addAddressListProvider(tcp);

		PingFailureDetection fail = new PingFailureDetection(udp, config);
		fail.addPingProvider(tcp);

		Stack tcpStack = Stack.newStack(tcp, udp, frag, mcast, data, disco, fail);
		tcpStack.setFD(fail);
		tcpStack.setDisco(disco);
		tcpStack.setData(data);
		// tcpStack.setStreamer(tcp);
		return tcpStack;
	}

	public void setMetrics(Metrics metrics) {
		for (StackElement se : stackElements) {
			se.setMetrics(metrics);
		}
	}

	public static Stack rabbitStack(NetworkConfiguration config, Address local, String name) {
		String iface = NetworkUtils.getFirstHostAddress(true);

		RabbitProcessor zmq = new RabbitProcessor(config, iface, local);

		NetworkProtocol mcast = NetworkProtocolFactory.mcast(local, config).getProtocol(iface);

		DataProcessor data = new DataProcessor(zmq, config);

		MultiCastDiscovery disco = new MultiCastDiscovery(local, name, config, mcast, zmq);
		disco.addAddressListProvider(zmq);

		PingFailureDetection fail = new PingFailureDetection(zmq, config);

		Stack tcpStack = Stack.newStack(zmq, mcast, data, disco, fail);
		tcpStack.setFD(fail);
		tcpStack.setDisco(disco);
		tcpStack.setData(data);
		return tcpStack;
	}

	public static Stack localStack(NetworkConfiguration config, Address address, String name) {
		LoopbackMP local = new LoopbackMP();
		DataProcessor data = new DataProcessor(local, config);
		MultiCastDiscovery disco = new MultiCastDiscovery(address, name, config, local, local);
		Stack tcpStack = Stack.newStack(local, disco, data);
		tcpStack.setDisco(disco);
		tcpStack.setData(data);
		return tcpStack;
	}

	@Override
	public Iterator<StackElement> iterator() {
		return stackElements.iterator();
	}
}