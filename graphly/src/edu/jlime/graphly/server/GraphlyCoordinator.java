package edu.jlime.graphly.server;

import edu.jlime.graphly.client.ConsistentHashing;

public interface GraphlyCoordinator {
	public ConsistentHashing getHash() throws Exception;
}
