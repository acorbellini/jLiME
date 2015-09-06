package edu.jlime.rpc.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import edu.jlime.core.stream.RemoteInputStream;

public class TCPInputStream extends RemoteInputStream {

	private InputStream is;

	public TCPInputStream(UUID streamId, InputStream is) {
		super(streamId);
		this.is = is;
	}

	@Override
	public int read() throws IOException {
		return is.read();
	}

	@Override
	public int read(byte[] b) throws IOException {
		return is.read(b);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return is.read(b, off, len);
	}

	@Override
	public void close() throws IOException {
		super.close();
		is.close();
	}

	public InputStream getIs() {
		return is;
	}

}