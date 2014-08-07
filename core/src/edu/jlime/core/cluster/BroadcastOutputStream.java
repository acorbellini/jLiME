package edu.jlime.core.cluster;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import edu.jlime.core.stream.RemoteOutputStream;

public class BroadcastOutputStream extends OutputStream {

	List<RemoteOutputStream> streams;

	public BroadcastOutputStream(List<RemoteOutputStream> streams2) {
		this.streams = streams2;
	}

	@Override
	public void write(int b) throws IOException {
		for (OutputStream dos : streams)
			dos.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		for (OutputStream dos : streams)
			dos.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		for (OutputStream dos : streams)
			dos.write(b, off, len);
	}

	@Override
	public void flush() throws IOException {
		for (OutputStream dos : streams)
			dos.flush();
	}

	@Override
	public void close() throws IOException {
		for (OutputStream dos : streams)
			dos.close();
	}
}
