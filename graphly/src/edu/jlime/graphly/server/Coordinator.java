package edu.jlime.graphly.server;

import edu.jlime.graphly.client.ConsistentHashing;

public interface Coordinator {
	public ConsistentHashing getHash() throws Exception;
}
