package edu.jlime.core.transport;

import java.util.UUID;

import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;

public interface Streamer {

	public RemoteInputStream getInputStream(UUID streamId, Address from);

	public RemoteOutputStream getOutputStream(UUID streamId, Address to);
}
