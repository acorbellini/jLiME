package edu.jlime.core.stream;

import java.io.OutputStream;
import java.util.UUID;

public abstract class RemoteOutputStream extends OutputStream {

	UUID streamID;

	public RemoteOutputStream(UUID streamID) {
		this.streamID = streamID;
	}
}
