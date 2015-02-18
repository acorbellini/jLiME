package edu.jlime.graphly.server;

import java.util.Map;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.client.ConsistentHashing;

public interface GraphlyCoordinatorBroadcast {

	public Map<Peer, ConsistentHashing> getHash() throws Exception;

}