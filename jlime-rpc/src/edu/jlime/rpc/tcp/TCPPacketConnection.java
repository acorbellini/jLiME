package edu.jlime.rpc.tcp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

class TCPPacketConnection {

	Logger log = Logger.getLogger(TCPPacketConnection.class);

	long lastTimeUsed;

	Socket conn;

	DataInputStream is;

	DataOutputStream os;

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
		is = new DataInputStream(new BufferedInputStream(sock.getInputStream(),
				input_buffer));
		os = new DataOutputStream(new BufferedOutputStream(
				sock.getOutputStream(), output_buffer));
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
			os.writeInt(id);
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
		byte[] data = null;
		try {
			if (log.isDebugEnabled())
				log.debug("WAITING FOR LOCK ON " + readLock + " "
						+ readLock.getQueueLength());
			readLock.lock();
			if (log.isDebugEnabled())
				log.debug("READING ON SOCKET " + conn);
			int id = is.readInt();
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
				data = new byte[id];
				if (log.isDebugEnabled())
					log.debug(" Fully reading from " + conn + " " + data.length
							+ " bytes.");
				is.readFully(data);
				this.lastTimeUsed = System.currentTimeMillis();
				if (log.isDebugEnabled())
					log.debug("FINISHED READING ON SOCKET " + conn);
				return data;
			}
		} catch (Exception e) {
			if (log.isDebugEnabled())
				log.debug(conn + " is closed.");
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