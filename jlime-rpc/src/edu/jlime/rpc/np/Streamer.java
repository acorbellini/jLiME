package edu.jlime.rpc.np;

import java.util.UUID;

import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.rpc.message.Address;

public interface Streamer {

	public RemoteInputStream getInputStream(UUID streamId, Address from);

	public RemoteOutputStream getOutputStream(UUID streamId, Address to);
}
