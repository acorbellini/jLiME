package edu.jlime.rpc.discovery;

import java.util.Map;

import edu.jlime.core.transport.Address;

public interface DiscoveryListener {

	void memberMessage(Address from, String name, Map<String, String> data)
			throws Exception;
}