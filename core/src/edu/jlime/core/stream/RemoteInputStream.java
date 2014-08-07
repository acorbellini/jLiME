package edu.jlime.core.stream;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.UUID;

public abstract class RemoteInputStream extends InputStream {

	UUID streamId;

	public RemoteInputStream(UUID streamId) {
		this.streamId = streamId;
	}

	public static DataInputStream getBDIS(RemoteInputStream inputStream, int i) {
		return new DataInputStream(new BufferedInputStream(inputStream, 4096));
	}

}
