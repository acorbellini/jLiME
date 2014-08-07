package edu.jlime.core.stream;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.util.UUID;

public abstract class RemoteOutputStream extends OutputStream {

	UUID streamID;

	public RemoteOutputStream(UUID streamID) {
		this.streamID = streamID;
	}

	public static DataOutputStream getBDOS(OutputStream os, int i) {
		return new DataOutputStream(new BufferedOutputStream(os, 4096));
	}
}
