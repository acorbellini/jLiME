package edu.jlime.core.stream;

import java.io.InputStream;
import java.util.UUID;

public abstract class RemoteInputStream extends InputStream {

	UUID streamId;

	public RemoteInputStream(UUID streamId) {
		this.streamId = streamId;
	}
}
