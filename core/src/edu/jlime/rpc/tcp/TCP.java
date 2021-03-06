package edu.jlime.rpc.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.DataReceiver;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.rpc.np.NetworkProtocol;
import edu.jlime.util.ByteBuffer;

public class TCP extends NetworkProtocol implements DataReceiver {

	TCPConfig config;

	Logger log = Logger.getLogger(TCP.class);

	private ConcurrentHashMap<Address, TCPConnectionManager> connections = new ConcurrentHashMap<>();

	private ConcurrentHashMap<Address, SocketAddress> lastAddress = new ConcurrentHashMap<>();

	private ConcurrentHashMap<Address, HashMap<UUID, InputStream>> streams = new ConcurrentHashMap<>();

	public TCP(Address id, String addr, int port, int range, TCPConfig config) {
		super(addr, port, range, new TCPSocketFactory(config.tcp_rcv_buffer), id);
		this.config = config;
	}

	@Override
	public List<SocketAddress> getAddresses() {
		List<SocketAddress> addr = new ArrayList<>();
		addr.add(getAddress());
		return addr;
	}

	SocketAddress getAddress() {
		InetSocketAddress sockAddr = (InetSocketAddress) getServerSocket().getLocalSocketAddress();
		return new SocketAddress(sockAddr, getType());
	}

	@Override
	public void onStart(Object sock) {
		Thread t = new Thread("TCP Connection Accepter") {
			@Override
			public void run() {
				while (!stopped)
					try {
						acceptConnection();
					} catch (Exception e) {
						if (log.isDebugEnabled())
							log.debug("Could not accept connection: " + e.getMessage());
					}
			}
		};
		// t.setDaemon(true);
		t.start();
	}

	private void setFlags(Socket sock) throws SocketException {
		sock.setTcpNoDelay(true);
		sock.setReceiveBufferSize(config.tcp_rcv_buffer);
		sock.setSendBufferSize(config.tcp_send_buffer);
	}

	protected void acceptConnection() throws Exception {
		final Socket conn = getServerSocket().accept();
		// TODO Careful
		setFlags(conn);
		InputStream inputStream = conn.getInputStream();
		StreamType type = StreamType.fromID((byte) inputStream.read());
		UUID id = TCPConnectionManager.getID(inputStream);
		if (type.equals(StreamType.PACKET)) {
			TCPConnectionManager connList = getConnManager(new Address(id));
			if (log.isDebugEnabled())
				log.debug("Received connection request from " + conn.getRemoteSocketAddress() + " with id " + id);
			connList.addConnection(conn);
		} else if (type.equals(StreamType.STREAM)) {
			if (log.isDebugEnabled())
				log.debug("Received stream request from " + conn.getRemoteSocketAddress() + " with id " + id);
			UUID streamID = TCPConnectionManager.getID(inputStream);
			Address id2 = new Address(id);
			addStream(streamID, inputStream, id2);
		}
	}

	private void addStream(UUID streamID, InputStream is, Address id2) {
		HashMap<UUID, InputStream> streams = getStreamOf(id2);
		synchronized (streams) {
			if (!streams.containsKey(streamID)) {
				streams.put(streamID, is);
				streams.notifyAll();
			}
			// else
			// log.warn("Connection ignored from "
			// + conn.getRemoteSocketAddress()
			// + " with Stream ID " + streamID);
		}
	}

	private HashMap<UUID, InputStream> getStreamOf(Address id) {
		HashMap<UUID, InputStream> streamsOfOrigin = streams.get(id);
		if (streamsOfOrigin == null)
			synchronized (streams) {
				streamsOfOrigin = streams.get(id);
				if (streamsOfOrigin == null) {
					streamsOfOrigin = new HashMap<>();
					streams.put(id, streamsOfOrigin);
				}
			}
		return streamsOfOrigin;
	}

	@Override
	public void sendBytes(final byte[] built, final Address to, final SocketAddress realSockAddr) throws Exception {

		// if (log.isDebugEnabled())
		// log.debug("Sending " + built.length + " bytes to " + to);
		final TCPConnectionManager mgr = getConnManager(to);
		SocketAddress toSend = null;
		if (realSockAddr != null) {
			toSend = realSockAddr;
		} else {
			toSend = getBestAddress(to);
		}

		if (toSend == null)
			throw new Exception("Can't find address for " + to + " given realsockaddr is " + realSockAddr);

		// if (!isEqualToLocalType(toSend.getSockTo())) {
		// if (log.isDebugEnabled())
		// log.debug("Won't send to different type of address " + toSend
		// + " != " + getAddr() + " REAL SOCKET : " + realSockAddr);
		// return;
		// }
		mgr.send(built, toSend);

	}

	private SocketAddress getBestAddress(final Address to) {
		SocketAddress toSend = lastAddress.get(to);
		if (toSend == null) {
			List<SocketAddress> bup = backup.get(to);
			if (bup != null)
				toSend = bup.get((int) (Math.random() * bup.size()));
		}
		return toSend;
	}

	private TCPConnectionManager getConnManager(Address to) {
		TCPConnectionManager mgr = connections.get(to);
		if (mgr == null)
			synchronized (connections) {
				mgr = connections.get(to);
				if (mgr == null) {
					mgr = new TCPConnectionManager(to, getLocal(), this, config);
					connections.put(to, mgr);
				}
			}
		return mgr;
	}

	private ServerSocket getServerSocket() {
		return (ServerSocket) getSocket().getJavaSocket();
	}

	@Override
	public void dataReceived(byte[] array, InetSocketAddress addr) throws Exception {
		// if (log.isDebugEnabled())
		// log.debug("Data (" + array.length + "b) received from " + addr);
		// if (!isEqualToLocalType(addr)) {
		// if (log.isDebugEnabled())
		// log.debug("Won't RECEIVE data from different address type.");
		// return;
		// }
		notifyPacketRvcd(new ByteBuffer(array), addr);
	}

	@Override
	protected void beforeProcess(ByteBuffer pkt, InetSocketAddress addr, Address from, Address to) {
		if (lastAddress.get(from) != null && lastAddress.get(from).getSockTo().getAddress().equals(addr.getAddress())
				&& lastAddress.get(from).getSockTo().getPort() == addr.getPort())
			return;
		List<SocketAddress> possibleAddressList = backup.get(from);
		if (possibleAddressList != null)
			for (SocketAddress add : possibleAddressList)
				if (addr.getAddress().equals(add.getSockTo().getAddress())
						&& addr.getPort() == add.getSockTo().getPort()) {
					if (log.isDebugEnabled())
						log.debug("Changing last address of  " + from + " to " + addr);
					lastAddress.put(from, new SocketAddress(addr, getType()));
				}

	}

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		TCPConnectionManager mgr = connections.get(addr);
		if (mgr != null)
			mgr.stop();
	}

	@Override
	public AddressType getType() {
		return AddressType.TCP;
	}

	public SocketAddress getLastAddress(Address to) {
		return lastAddress.get(to);
	}

	@Override
	public RemoteInputStream getInputStream(UUID streamId, Address from) {
		HashMap<UUID, InputStream> streams = getStreamOf(from);
		InputStream is = null;
		synchronized (streams) {
			while ((is = streams.get(streamId)) == null) {
				try {
					streams.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return new TCPInputStream(streamId, is);
	}

	@Override
	public RemoteOutputStream getOutputStream(UUID streamId, final Address to) {
		if (getLocal().equals(to)) {
			try {
				final PipedInputStream is = new PipedInputStream(32 * 1024);
				final PipedOutputStream bos = new PipedOutputStream(is);
				RemoteOutputStream os = new RemoteOutputStream(streamId) {
					@Override
					public void write(int b) throws IOException {
						bos.write(b);
					}

					@Override
					public void write(byte[] b) throws IOException {
						bos.write(b);
					}

					@Override
					public void write(byte[] b, int off, int len) throws IOException {
						bos.write(b, off, len);
					}

					@Override
					public void close() throws IOException {
						bos.close();
					}
				};
				addStream(streamId, is, to);
				return os;
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		int tries = 0;
		while (tries < 3)
			try {
				SocketAddress addr = getBestAddress(to);
				// A new connection but using it as a stream.
				Socket sock = new Socket(addr.getSockTo().getAddress(), addr.getSockTo().getPort());
				if (log.isDebugEnabled())
					log.debug("Created Streaming Socket " + sock + " to " + addr);

				// TODO Careful

				sock.setReceiveBufferSize(config.tcp_rcv_buffer);
				// System.out.println(sock.getReceiveBufferSize());

				sock.setSendBufferSize(config.tcp_send_buffer);
				// System.out.println(sock.getSendBufferSize());
				// sock.setTcpNoDelay(true);
				OutputStream outputStream = sock.getOutputStream();
				outputStream.write(StreamType.STREAM.getId());
				ByteBuffer localBytes = new ByteBuffer();
				localBytes.putUUID(getLocal().getId());
				outputStream.write(localBytes.build());
				ByteBuffer streamBytes = new ByteBuffer();
				streamBytes.putUUID(streamId);
				outputStream.write(streamBytes.build());
				outputStream.flush();
				return new TCPOutputStream(streamId, sock, outputStream);
			} catch (Exception e) {
				e.printStackTrace();
				tries++;
			}
		return null;

	}

	@Override
	public void stopNP() throws Exception {
		getServerSocket().close();
		for (TCPConnectionManager e : connections.values()) {
			e.stop();
		}
	}

	@Override
	public String toString() {
		return "TCP " + getAddr() + "/" + getPort();
	}

}
