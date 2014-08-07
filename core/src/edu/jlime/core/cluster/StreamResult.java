package edu.jlime.core.cluster;

import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;

public class StreamResult {

	RemoteOutputStream os;

	RemoteInputStream is;

	public StreamResult(RemoteOutputStream os, RemoteInputStream is) {
		super();
		this.os = os;
		this.is = is;
	}

	public RemoteOutputStream getOs() {
		return os;
	}

	public RemoteInputStream getIs() {
		return is;
	}
}
