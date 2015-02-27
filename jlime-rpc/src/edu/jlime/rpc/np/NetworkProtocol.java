package edu.jlime.rpc.np;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.IP;
import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.Streamer;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.AddressListProvider;
import edu.jlime.rpc.SocketFactory;
import edu.jlime.rpc.SocketFactory.jLimeSocket;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.util.Buffer;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.PerfMeasure;
import edu.jlime.util.RingQueue;

public abstract class NetworkProtocol extends SimpleMessageProcessor implements
		AddressListProvider, Streamer {

	private Logger log = Logger.getLogger(NetworkProtocol.class);

	protected ConcurrentHashMap<Address, List<SocketAddress>> backup = new ConcurrentHashMap<>();

	// private RingQueue packetsRx = new RingQueue();

	private String addr;

	private int port;

	private int portrange;

	private SocketAddress localAddr;

	private Address local;

	private jLimeSocket socket;

	protected SocketFactory fact;

	private Metrics metrics;

	public NetworkProtocol(String addr, int port, int range,
			SocketFactory fact, Address id) {
		super(null, "Network Protocol");
		this.setLocal(id);
		this.fact = fact;
		this.setAddr(addr);
		this.port = port;
		this.portrange = range;
		// Thread t = new Thread("Network Protocol Data Packet Reader") {
		// @Override
		// public void run() {
		// while (!stopped)
		// try {
		// Object[] pack = packetsRx.take();
		// if (stopped)
		// return;
		// for (Object object : pack) {
		// processPacket((DataPacket) object);
		// }
		//
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// }
		// };
		// t.setDaemon(true);
		// t.start();
	}

	public void notifyPacketRvcd(DataPacket pkt) throws Exception {
		// packetsRx.put(pkt);
		processPacket(pkt);
	};

	private void processPacket(DataPacket pkt) throws Exception {
		Buffer buff = pkt.reader;
		Address from = new Address(buff.getUUID());
		Address to = new Address(buff.getUUID());

		beforeProcess(pkt, from, to);

		if (!to.equals(Address.noAddr()) && !to.equals(getLocal())) {
			// if (log.isDebugEnabled())
			// log.debug("Message from " + from + " to " + to
			// + " wasn't for me (" + getLocal() + ")");
			return;
		}
		byte[] data = buff.getRawByteArray();

		Message msg = Message.deEncapsulate(data, from, getLocal());
		// if (log.isTraceEnabled())
		// log.debug("Received message of type " + msg.getType() + " sized "
		// + buff.size() + " bytes from " + from + " with address "
		// + addr);
		notifyRcvd(msg);

	}

	protected abstract void beforeProcess(DataPacket pkt, Address from,
			Address to);

	public void onStart() throws Exception {

		int tries = 0;
		while (getSocket() == null && tries != portrange) {
			try {
				socket = fact.getSocket(getAddr(), port + tries);
			} catch (Exception e) {
				tries++;
			}
		}

		if (socket == null)
			throw new Exception("Could not set port from " + port + " to "
					+ (port + portrange));

		if (metrics != null)
			this.metrics.set("jlime.interface").update(
					this.socket.getAddr() + ":" + this.socket.getPort());

		port = port + tries;

		if (log.isDebugEnabled())
			log.debug("Socket TYPE " + getType() + " Created : " + getAddr()
					+ " with port " + port + " on peer " + local);

		localAddr = new SocketAddress(new InetSocketAddress(
				InetAddress.getByName(getAddr()), port), getType());

		onStart(getSocket().getJavaSocket());
	}

	public abstract AddressType getType();

	@Override
	public void send(Message msg) throws Exception {
		Address to = msg.getTo();
		SocketAddress realSockAddr = null;
		if (to == null) {
			to = Address.noAddr();
			realSockAddr = localAddr;
		} else if (to != null && msg.hasSock()) {
			realSockAddr = msg.getSock();
		}

		byte[] array = msg.toByteArray();

		ByteBuffer writer = new ByteBuffer(array.length + 32);
		writer.putUUID(getLocal().getId());
		writer.putUUID(to.getId());
		writer.putRawByteArray(array);
		byte[] built = writer.build();

		// if (log.isTraceEnabled())
		// log.trace("Sending message from " + getLocal() + " to " + to
		// + " using this message info: TYPE " + msg.getType()
		// + " SIZE: " + built.length + " bytes TO " + msg.getTo()
		// + " ADDRESS " + realSockAddr);

		sendBytes(built, to, realSockAddr);
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getAddr() {
		return addr;
	}

	public void setAddr(String addr) {
		this.addr = addr;
	}

	public abstract void onStart(Object sock);

	public abstract void sendBytes(byte[] built, Address to,
			SocketAddress realSockAddr) throws Exception;

	public jLimeSocket getSocket() {
		return socket;
	}

	public Address getLocal() {
		return local;
	}

	public void setLocal(Address id) {
		this.local = id;
	}

	@Override
	public void onStop() throws Exception {
		if (log.isDebugEnabled())
			log.debug("Stopping network protocol type " + getType()
					+ " and socket " + socket);

		// packetsRx.put(new DataPacket(null, null));
		if (metrics != null)
			metrics.set("jlime.interface").remove(
					this.socket.getAddr() + ":" + this.socket.getPort());
	}

	protected boolean isEqualToLocalType(InetSocketAddress addr) {
		String toAddr = addr.getAddress().getHostAddress();
		IP ip = IP.toIP(toAddr);
		IP ipLocal = IP.toIP(getAddr());
		if (!ip.getType().equals(ipLocal.getType()))
			return false;
		return true;
	}

	@Override
	public void updateAddress(Address id, List<SocketAddress> addresses) {
		if (log.isDebugEnabled())
			log.info("Updating addresses for address " + id + " with "
					+ addresses);
		List<SocketAddress> update = new ArrayList<>();
		for (SocketAddress socketAddress : addresses) {
			if (isEqualToLocalType(socketAddress.getSockTo()))
				update.add(socketAddress);
		}
		backup.put(id, update);
	}

	@Override
	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;

	}
}
