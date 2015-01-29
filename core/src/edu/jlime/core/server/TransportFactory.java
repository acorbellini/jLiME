package edu.jlime.core.server;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.transport.Transport;

public interface TransportFactory {

	Transport build(Peer p);
}
