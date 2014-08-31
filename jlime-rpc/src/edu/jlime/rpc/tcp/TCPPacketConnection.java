package edu.jlime.rpc.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.StreamUtils;

class TCPPacketConnection {

	Logger log = Logger.getLogger(TCPPacketConnection.class);

	long lastTimeUsed;

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
		this.lastTimeUsed = System.currentTimeMillis();
		is = sock.getInputStream();
		os = sock.getOutputStream();
	}

	// protected void setClosed() {
	// if (log.isDebugEnabled())
	// log.debug("Sending Connection close init " + conn);
	// synchronized (this) {
	// if (!write(CONN_CLOSE_INIT))
	// stop();
	// closed = true;
	// }
	// }

	public void write(byte[] out, int id) throws Exception {
		try {
			writeLock.lock();
			if (closed) {
				if (log.isDebugEnabled())
					log.debug("Writing is forbidden on " + conn);
				return;
			}
			if (log.isDebugEnabled())
				log.debug("Writing to " + out.length + " bytes to " + conn);
			os.write(DataTypeUtils.intToByteArray(id));
			this.lastTimeUsed = System.currentTimeMillis();
			if (id >= 0)
				os.write(out);
			this.lastTimeUsed = System.currentTimeMillis();
			os.flush();
			if (log.isDebugEnabled())
				log.debug("Finished Writing to " + conn);
			return;
		} catch (Exception e) {
			throw new Exception("Error WRITING to " + conn, e);
		} finally {
			writeLock.unlock();
		}
	}

	public byte[] read() throws Exception {
		try {
			if (log.isDebugEnabled())
				log.debug("WAITING FOR LOCK ON " + readLock + " "
						+ readLock.getQueueLength());
			readLock.lock();
			if (log.isDebugEnabled())
				log.debug("READING ON SOCKET " + conn);
			int id = StreamUtils.readInt(is);
			if (log.isDebugEnabled())
				log.debug("Read " + (id < 0 ? " ID " : " SIZE ") + id);
			// if (id == CONN_CLOSE_INIT) {
			// if (log.isDebugEnabled())
			// log.debug("Received connection close init, sending response "
			// + conn);
			// synchronized (this) {
			// closeTimer.cancel();
			// closed = false;
			// write(null, CONN_CLOSE_RESP);
			// closed = true;
			// if (log.isDebugEnabled())
			// log.debug("Received Connection response, removing connection "
			// + conn);
			// conn.close();
			// mgr.remove(this);
			// }
			// } else if (id == CONN_CLOSE_RESP) {
			// if (log.isDebugEnabled())
			// log.debug("Received connection close response " + conn);
			// synchronized (this) {
			// closed = true;
			// conn.close();
			// mgr.remove(this);
			// }
			// } else {
			this.lastTimeUsed = System.currentTimeMillis();
			if (log.isDebugEnabled())
				log.debug(" Fully reading from " + conn + " " + id + " bytes.");
			byte[] data = StreamUtils.read(is, id);
			this.lastTimeUsed = System.currentTimeMillis();
			if (log.isDebugEnabled())
				log.debug("FINISHED READING ON SOCKET " + conn);
			return data;
			// }
		} catch (Exception e) {
			throw new Exception("Error reading from " + conn, e);
		} finally {
			readLock.unlock();
		}
		// return null;
	}

	@Override
	public String toString() {
		return this.conn + "/" + this.lastTimeUsed / 1000;
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
}