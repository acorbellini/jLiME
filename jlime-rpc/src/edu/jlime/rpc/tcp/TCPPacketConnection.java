package edu.jlime.rpc.tcp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import edu.jlime.util.ByteBuffer;
import edu.jlime.util.IntUtils;
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

	public TCPPacketConnection(Socket sock, Timer closer,
			final long time_limit, TCPConnectionManager mgr, int input_buffer,
			int output_buffer) throws IOException {
		this.mgr = mgr;
		this.conn = sock;
		this.lastTimeUsed = System.currentTimeMillis();
		// is = new BufferedInputStream(sock.getInputStream(), input_buffer);
		is = sock.getInputStream();
		os = new BufferedOutputStream(sock.getOutputStream());
		closeTimer = new TimerTask() {

			@Override
			public void run() {

				long curr = System.currentTimeMillis();
				if (curr - lastTimeUsed > time_limit) {
					setClosed();
					cancel();
				}
			}
		};
		// closer.schedule(closeTimer, time_limit, time_limit);
	}

	protected void setClosed() {
		if (log.isDebugEnabled())
			log.debug("Sending Connection close init " + conn);
		synchronized (this) {
			if (!write(CONN_CLOSE_INIT))
				stop();
			closed = true;
		}
	}

	private boolean write(int id) {
		return write(null, id);
	}

	public boolean write(byte[] out) {
		return write(out, out.length);
	}

	private boolean write(byte[] out, int id) {
		try {
			writeLock.lock();
			if (closed) {
				if (log.isDebugEnabled())
					log.debug("Writing is forbidden on " + conn);
				return false;
			}
			if (log.isDebugEnabled())
				log.debug("Writing to " + conn);
			os.write(IntUtils.intToByteArray(id));
			this.lastTimeUsed = System.currentTimeMillis();
			if (id >= 0)
				os.write(out);
			this.lastTimeUsed = System.currentTimeMillis();
			os.flush();
			if (log.isDebugEnabled())
				log.debug("Finished Writing to " + conn);
			return true;
		} catch (Exception e) {
			if (log.isDebugEnabled())
				log.debug("Error WRITING to " + conn + " (Socket closed?)", e);
			return false;
		} finally {
			writeLock.unlock();
		}
	}

	public byte[] read() {

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
			if (id == CONN_CLOSE_INIT) {
				if (log.isDebugEnabled())
					log.debug("Received connection close init, sending response "
							+ conn);
				synchronized (this) {
					closeTimer.cancel();
					closed = false;
					write(CONN_CLOSE_RESP);
					closed = true;
					if (log.isDebugEnabled())
						log.debug("Received Connection response, removing connection "
								+ conn);
					conn.close();
					mgr.remove(this);
				}
			} else if (id == CONN_CLOSE_RESP) {
				if (log.isDebugEnabled())
					log.debug("Received connection close response " + conn);
				synchronized (this) {
					closed = true;
					conn.close();
					mgr.remove(this);
				}
			} else {
				this.lastTimeUsed = System.currentTimeMillis();
				if (log.isDebugEnabled())
					log.debug(" Fully reading from " + conn + " " + id
							+ " bytes.");
				byte[] data = StreamUtils.read(is, id);
				this.lastTimeUsed = System.currentTimeMillis();
				if (log.isDebugEnabled())
					log.debug("FINISHED READING ON SOCKET " + conn);
				return data;
			}
		} catch (Exception e) {
			if (log.isDebugEnabled())
				log.debug("Error reading from " + conn, e);
		} finally {
			readLock.unlock();
		}
		return null;
	}

	@Override
	public String toString() {
		return this.conn + "/" + this.lastTimeUsed / 1000;
	}

	public void stop() {
		try {
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}