package edu.jlime.rpc.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.RingQueue;
import edu.jlime.util.StreamUtils;

class TCPConnectionManager {

	// private ExecutorService rcv;

	// private ExecutorService exec = Executors
	// .newCachedThreadPool(new ThreadFactory() {
	//
	// @Override
	// public Thread newThread(Runnable r) {
	// Thread t = Executors.defaultThreadFactory().newThread(r);
	// t.setName("TCP Worker Thread");
	// return t;
	// }
	// });

	private ExecutorService connection_exec = Executors
			.newCachedThreadPool(new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setName("TCP Connection Reader");
					return t;
				}
			});

	private Logger log = Logger.getLogger(TCPConnectionManager.class);

	private boolean stopped = false;

	TCP rcvr;

	private List<TCPPacketConnection> connections = new CopyOnWriteArrayList<TCPPacketConnection>();

	private int conn_limit = 1;

	private long time_limit = 15000;

	private Address to;

	Address localID;

	private int input_buffer;

	private int output_buffer;

	private Thread receiver;

	RingQueue packets = new RingQueue();

	// RingQueue writeQueue = new RingQueue();

	private Thread reader;

	public TCPConnectionManager(Address addr, Address localID, TCP rcvr,
			TCPConfig config) {
		this.conn_limit = config.conn_limit;
		this.time_limit = config.time_limit;
		this.input_buffer = config.input_buffer;
		this.output_buffer = config.output_buffer;
		this.rcvr = rcvr;
		this.to = addr;
		this.localID = localID;

		// Thread t = new Thread("Writer Queue reader for " + to) {
		// @Override
		// public void run() {
		// while (!stopped) {
		// Object[] list = writeQueue.take();
		// if (stopped)
		// return;
		// for (Object pkt : list) {
		// writeToConn((OutPacket) pkt);
		// }
		// }
		// }
		// };
		// t.start();

		// reader = new Thread("TCP Packet Reader") {
		// @Override
		// public void run() {
		// while (!stopped) {
		// Object[] list = packets.take();
		// for (Object object : list) {
		// final TCPPacket pkt = (TCPPacket) object;
		// if (pkt.d == null)
		// return;
		// exec.execute(new Runnable() {
		//
		// @Override
		// public void run() {
		// try {
		// TCPConnectionManager.this.rcvr
		// .dataReceived(
		// pkt.d,
		// (InetSocketAddress) pkt.c.conn
		// .getRemoteSocketAddress());
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }
		// });
		// }
		// }
		// }
		// };
		// reader.start();

		// receiver = new Thread("TCP Connection Reader") {
		// @Override
		// public void run() {
		// while (!stopped) {
		// boolean read = false;
		// for (final TCPPacketConnection c : connections) {
		// try {
		// if (c.try_read()) {
		// read = true;
		// final byte[] d = c.getBuffer();
		// if (d != null) {
		// packets.put(new TCPPacket(d, c));
		// }
		// }
		// } catch (Exception e) {
		// if (log.isDebugEnabled())
		// log.debug("Error reading from " + c
		// + " removing connection.: "
		// + e.getMessage());
		// remove(c);
		// }
		// }
		// // if (cont > 100000) {
		// // cont = 0;
		// if (!read)
		// try {
		// Thread.sleep(5);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// }
		// }
		// };
		// receiver.start();

		// closer = new Timer("Connection Closer");
		// Thread t = new Thread("Writer Queue reader for " + to) {
		// @Override
		// public void run() {
		// while (!stopped) {
		// Object[] list = writeQueue.take();
		// if (stopped)
		// return;
		// for (Object pkt : list) {
		// writeToConn((OutPacket) pkt);
		// }
		// }
		// }
		// };
		// t.start();
	}

	protected void writeToConn(final OutPacket pkt) {
		// send.execute(new Runnable() {
		// @Override
		// public void run() {
		boolean done = false;
		while (!done && !stopped) {
			SocketAddress addr = pkt.addr;
			TCPPacketConnection bestConn = getConnection(addr);
			// TODO esto no es correcto, deber�a cancelar el env�o del
			// paquete.
			if (bestConn == null) {
				// writeQueue.put(pkt);
				// writeToConn(pkt);
				log.warn("Couldn't send packet " + pkt
						+ " no available connections to destination.");
				done = true;
			} else {
				// if (log.isDebugEnabled())
				// log.debug("Sending " + pkt.data.length + "b using " +
				// bestConn
				// + " to " + to);

				try {
					// PerfMeasure.startTimer("mgr", 1000);
					bestConn.write(pkt.data, pkt.data.length);
					done = true;
					// PerfMeasure.takeTime("mgr");
				} catch (Exception e) {
					if (log.isDebugEnabled())
						log.debug("Error sending to " + addr + " using "
								+ bestConn
								+ ". Removing connection and trying again.");
					remove(bestConn);
				}
			}
		}
		// }
		// });

	}

	private static class OutPacket {

		byte[] data;

		SocketAddress addr;

		public OutPacket(byte[] data, SocketAddress addr) {
			super();
			this.data = data;
			this.addr = addr;
		}

	}

	public void send(final byte[] data, SocketAddress realSockAddr) {
		if (!realSockAddr.getType().equals(AddressType.TCP)) {
			if (log.isDebugEnabled())
				log.debug("Won't send a packet to " + realSockAddr);
			return;
		}
		writeToConn(new OutPacket(data, realSockAddr));
		// writeQueue.put(new OutPacket(data, realSockAddr));
	}

	private TCPPacketConnection getConnection(SocketAddress addr) {
		if (connections.size() < conn_limit && addr != null) {
			synchronized (connections) {
				if (connections.size() < conn_limit && addr != null)
					createConnection(addr);
			}
		}
		if (connections.isEmpty())
			return null;
		TCPPacketConnection conn = connections
				.get((int) (Math.random() * connections.size()));
		return conn;

	}

	private TCPPacketConnection createConnection(SocketAddress addr) {
		int tries = 0;
		while (tries <= 3) {
			Socket sock = null;
			try {
				sock = new Socket();
				// TODO Careful

				// sock.setReuseAddress(true);

				setFlags(sock);
				sock.connect(new InetSocketAddress(addr.getSockTo()
						.getAddress(), addr.getSockTo().getPort()));

				if (log.isDebugEnabled())
					log.debug("Created socket " + sock + " to " + addr);
				OutputStream os = sock.getOutputStream();
				os.write(StreamType.PACKET.getId());
				ByteBuffer asbytes = new ByteBuffer();
				asbytes.putUUID(localID.getId());
				os.write(asbytes.build());
				os.flush();
				return addConnection(sock);
			} catch (ConnectException e) {
				if (log.isDebugEnabled())
					log.debug("Could not open socket to " + addr + " : "
							+ e.getMessage());
				return null;
			} catch (Exception e) {
				log.info("Could not open socket to " + addr + " socket is "
						+ sock + ", trying again in 1s : " + e.getMessage());
				tries++;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
		return null;
	}

	private void setFlags(Socket sock) throws SocketException {
		sock.setTcpNoDelay(true);
		sock.setReceiveBufferSize(rcvr.config.tcp_rcv_buffer);
		sock.setSendBufferSize(rcvr.config.tcp_send_buffer);
	}

	public TCPPacketConnection addConnection(final Socket conn)
			throws Exception {
		final TCPPacketConnection c = new TCPPacketConnection(conn, time_limit,
				this, input_buffer, output_buffer);
		connections.add(c);

		connection_exec.execute(c);
		// new Runnable() {
		//
		// @Override
		// public void run() {
		// while (!stopped) {
		// try {
		// byte[] d = c.read();
		// if (d != null) {
		// packets.put(new TCPPacket(d, c));
		// }
		// } catch (Exception e) {
		// if (log.isDebugEnabled())
		// log.debug("Error reading from " + c
		// + " removing connection.: "
		// + e.getMessage());
		// remove(c);
		// }
		// }
		// }
		// });

		return c;
	}

	public void stop() {
		// rcv.shutdown();
		// exec.shutdown();
		connection_exec.shutdown();
		stopped = true;
		// writeQueue.put(new OutPacket(new byte[] {}, null));
		packets.put(new TCPPacket(null, null));
		for (TCPPacketConnection c : connections)
			c.stop();

	}

	public static UUID getID(InputStream is) throws IOException {
		return new ByteBuffer(StreamUtils.read(is, 16)).getUUID();
	}

	@Override
	public String toString() {
		return "TCP Connection manager for " + to;
	}

	public void remove(TCPPacketConnection tcpConnection) {
		connections.remove(tcpConnection);
	}
}