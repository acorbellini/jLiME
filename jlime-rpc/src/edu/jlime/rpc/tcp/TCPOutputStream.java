package edu.jlime.rpc.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.jlime.core.stream.RemoteOutputStream;

public class TCPOutputStream extends RemoteOutputStream {

	Logger log = Logger.getLogger(TCPOutputStream.class);

	private OutputStream os;

	private Socket sock;

	public TCPOutputStream(UUID streamId, Socket sock, OutputStream os) {
		super(streamId);
		this.sock = sock;
		this.os = os;
	}

	@Override
	public void write(int b) throws IOException {
		os.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		os.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		os.write(b, off, len);
	}

	@Override
	public void close() throws IOException {
		if (log.isDebugEnabled())
			log.debug("Closing TCP socket and outputstream.");
		os.close();
		sock.close();
	}

	@Override
	public void flush() throws IOException {
		os.flush();
	}

	@Override
	public String toString() {
		return "OutputStream using socket " + sock;
	}
}