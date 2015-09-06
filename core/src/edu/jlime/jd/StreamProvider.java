package edu.jlime.jd;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;

public interface StreamProvider {

	RemoteInputStream getInputStream(UUID streamID, Peer streamSource);

	RemoteOutputStream getOutputStream(UUID streamID, Peer streamSource);

}
