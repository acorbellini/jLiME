package edu.jlime.rpc.udp;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.core.transport.Address;
import edu.jlime.rpc.SocketFactory;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.rpc.np.DataPacket;
import edu.jlime.rpc.np.NetworkProtocol;
import edu.jlime.util.Buffer;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.RingQueue;

public class UDP extends NetworkProtocol implements PacketReceiver {

	private RingQueue packetsTx = new RingQueue();

	private enum DatagramType {

		DATA((byte) 0),

		STREAM((byte) 1);

		private byte id;

		private DatagramType(byte id) {
			this.id = id;
		}

		public byte getId() {
			return id;
		}

		public static DatagramType fromID(byte id) {
			for (DatagramType dt : DatagramType.values())
				if (dt.id == id)
					return dt;
			return null;
		}
	}

	private static Logger log = Logger.getLogger(NetworkProtocol.class);

	protected HashMap<Address, SocketAddress> currentSendAddress = new HashMap<>();

	protected ConcurrentHashMap<Address, HashMap<UUID, LinkedBlockingDeque<byte[]>>> streamData = new ConcurrentHashMap<>();

	private int max_bytes = 2000;

	private boolean isMcast = false;

	private DatagramReceiver rx;

	public UDP(Address logical, String addr, int port, int range,
			int max_msg_size, SocketFactory fact) {
		this(logical, addr, port, range, max_msg_size, false, fact);
	}

	public UDP(Address logical, String addr, int port, int range,
			int max_msg_size, boolean mcast, SocketFactory fact) {
		super(addr, port, range, fact, logical);
		this.max_bytes = max_msg_size + 100;
	}

	@Override
	public void datagramReceived(DatagramPacket p) throws Exception {

		InetSocketAddress sockAddr = (InetSocketAddress) p.getSocketAddress();

		// if (!isEqualToLocalType(sockAddr)) {
		// log.info("Won't accept packet from other addr " + sockAddr
		// + " local is " + getAddr());
		// }

		Buffer buffer = new ByteBuffer(p.getData(), p.getLength());
		DatagramType dt = DatagramType.fromID(buffer.get());
		if (dt.equals(DatagramType.DATA)) {
			// byte[] data = new byte[p.getLength() - 1];
			// System.arraycopy(buffer.array(), 1, data, 0, data.length);

			notifyPacketRvcd(new DataPacket(new ByteBuffer(
					buffer.getRawByteArray()), sockAddr));
		} else if (dt.equals(DatagramType.STREAM)) {
			UUID streamID = buffer.getUUID();
			UUID fromID = buffer.getUUID();
			// byte[] data = new byte[p.getLength() - 17];
			// System.arraycopy(buffer.array(), 17, data, 0, data.length);
			addToStream(buffer.getRawByteArray(), streamID, new Address(fromID));
		}
	}

	private void addToStream(byte[] data, UUID streamID, Address from) {
		LinkedBlockingDeque<byte[]> list = null;
		HashMap<UUID, LinkedBlockingDeque<byte[]>> sd = getStreamDataOf(from);
		synchronized (sd) {
			list = sd.get(streamID);
			if (list == null) {
				list = new LinkedBlockingDeque<>();
				sd.put(streamID, list);
				sd.notifyAll();
			}
		}
		list.addFirst(data);
	}

	private HashMap<UUID, LinkedBlockingDeque<byte[]>> getStreamDataOf(
			Address from) {
		HashMap<UUID, LinkedBlockingDeque<byte[]>> sd = null;
		synchronized (streamData) {
			sd = streamData.get(from);
			if (sd == null) {
				sd = new HashMap<>();
				streamData.put(from, sd);
			}
		}
		return sd;
	}

	@Override
	public String toString() {
		return "UDP " + getAddr() + "/" + getPort();
	}

	@Override
	public void onStop() throws Exception {
		super.onStop();
		getDatagramSocket().close();
		packetsTx.put(new DatagramPacket(new byte[] {}, 0));
		rx.setStopped();
	}

	@Override
	public void sendBytes(byte[] built, Address to, SocketAddress realSockAddr)
			throws Exception {
		send(DatagramType.DATA, built, to, realSockAddr);
	}

	private void send(DatagramType data, byte[] bytes, Address to,
			SocketAddress realSockAddr) throws Exception {
		byte[] built = new byte[bytes.length + 1];
		built[0] = data.getId();
		System.arraycopy(bytes, 0, built, 1, bytes.length);

		if (realSockAddr != null && !realSockAddr.getType().equals(getType()))
			return;

		if (to != null && realSockAddr == null) {
			realSockAddr = currentSendAddress.get(to);
			if (realSockAddr == null) {
				List<SocketAddress> bup = backup.get(to);
				if (bup != null && !bup.isEmpty()) {
					realSockAddr = bup.get((int) (Math.random() * bup.size()));
				} else if (!to.equals(Address.noAddr())) {
					log.error("Address "
							+ to
							+ " was not in send table, and did not contain a physical address to send to.");
					return;
				}
			}
		}

		if (built.length > max_bytes) {
			log.error("Can not send message with size " + built.length);
			return;
		}
		DatagramPacket dg = new DatagramPacket(built, built.length,
				realSockAddr.getSockTo());
		send(getDatagramSocket(), dg);
	}

	@Override
	public List<SocketAddress> getAddresses() {
		List<SocketAddress> list = new ArrayList<>();
		InetSocketAddress sockAddr = (InetSocketAddress) getDatagramSocket()
				.getLocalSocketAddress();
		list.add(new SocketAddress(sockAddr, getType()));
		return list;
	}

	private DatagramSocket getDatagramSocket() {
		return (DatagramSocket) getSocket().getJavaSocket();
	}

	public void onStart(Object socket) {
		rx = new DatagramReceiver((DatagramSocket) socket, max_bytes, this);

	}

	private void send(DatagramSocket sock, Object obj) {
		DatagramPacket dg = (DatagramPacket) obj;
		InetAddress localAddress = sock.getLocalAddress();
		if (localAddress != null
				&& dg.getAddress().getClass().equals(localAddress.getClass()))
			try {
				sock.send(dg);
			} catch (Exception e) {
				log.debug("Failed sending datagram to " + dg.getAddress() + ":"
						+ dg.getPort() + " with size " + dg.getLength()
						+ " on socket " + sock.getLocalSocketAddress(), e);
			}
	}

	@Override
	public void beforeProcess(DataPacket pkt, Address from, Address to) {
		currentSendAddress.put(from,
				new SocketAddress(pkt.getAddr(), getType()));
	}

	@Override
	public AddressType getType() {
		return (isMcast ? AddressType.MCAST : AddressType.UDP);
	}

	@Override
	public RemoteInputStream getInputStream(final UUID streamId,
			final Address from) {
		return new RemoteInputStream(streamId) {
			byte[] currData;
			int offset = 0;

			@Override
			public int read() throws IOException {
				if (currData == null || offset == currData.length) {
					HashMap<UUID, LinkedBlockingDeque<byte[]>> streamData = getStreamDataOf(from);
					LinkedBlockingDeque<byte[]> list = null;
					synchronized (streamData) {
						list = streamData.get(streamId);
						while (list == null) {
							try {
								streamData.wait();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							list = streamData.get(streamId);
						}
					}
					try {
						currData = list.takeLast();
						offset = 0;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				return currData[offset++];
			}
		};
	}

	@Override
	public RemoteOutputStream getOutputStream(final UUID streamId,
			final Address to) {
		return new RemoteOutputStream(streamId) {
			java.nio.ByteBuffer buffer = java.nio.ByteBuffer
					.allocate(max_bytes - 16);
			boolean closed = false;

			@Override
			public void write(int b) throws IOException {
				if (!buffer.hasRemaining()) {
					flush();
				} else
					buffer.put((byte) b);

			}

			@Override
			public void flush() throws IOException {
				java.nio.ByteBuffer toSend = java.nio.ByteBuffer
						.allocate(max_bytes);
				toSend.putLong(streamId.getLeastSignificantBits());
				toSend.putLong(streamId.getMostSignificantBits());
				toSend.put(buffer.array());
				try {
					send(DatagramType.STREAM, toSend.array(), to, null);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			@Override
			public void close() throws IOException {
				flush();
				closed = true;
			}
		};
	}

}
