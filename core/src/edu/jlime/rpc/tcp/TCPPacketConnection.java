package edu.jlime.rpc.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.StreamUtils;

class TCPPacketConnection implements Runnable {

	Logger log = Logger.getLogger(TCPPacketConnection.class);
	Socket conn;

	InputStream is;

	OutputStream os;

	private static final int CONN_CLOSE_INIT = -1;

	private static final int CONN_CLOSE_RESP = -2;

	boolean closed = false;

	ReentrantLock readLock = new ReentrantLock();

	ReentrantLock writeLock = new ReentrantLock();

	private TCPConnectionManager mgr;

	private TimerTask closeTimer;

	public TCPPacketConnection(Socket sock, final long time_limit,
			TCPConnectionManager mgr, int input_buffer, int output_buffer)
			throws IOException {
		this.mgr = mgr;
		this.conn = sock;
		is = sock.getInputStream();
		os = sock.getOutputStream();
	}

	byte[] size = new byte[4];

	public void write(byte[] out, int id) throws Exception {
		try {
			writeLock.lock();
			if (closed) {
				return;
			}
			// os.writeInt(id);
			DataTypeUtils.intToByteArray(id, 0, size);
			os.write(size);
			if (id >= 0)
				// os.write(out);
				os.write(out);
			return;
		} catch (Exception e) {
			throw new Exception("Error WRITING to " + conn, e);
		} finally {
			writeLock.unlock();
		}
	}

	public byte[] read() throws Exception {
		try {
			int id = StreamUtils.readInt(is);
			byte[] data = StreamUtils.read(is, id);
			return data;
			// }
		} catch (Exception e) {
			throw new Exception("Error reading from " + conn, e);
		}
	}

	@Override
	public String toString() {
		return this.conn.toString();
	}

	public void stop() {
		try {
			if (log.isDebugEnabled())
				log.debug("Closing connection " + conn);
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		while (!closed) {
			try {
				// int size = is.readInt();
				// byte[] br = new byte[size];
				// is.readFully(br, 0, br.length);
				byte[] b = read();
				if (b != null) {
					mgr.rcvr.dataReceived(b,
							(InetSocketAddress) conn.getRemoteSocketAddress());
				}// mgr.packets.put(new TCPPacket(b, this));
					// else
					// return;
			} catch (Exception e) {
				return;
			}

		}
	}
}