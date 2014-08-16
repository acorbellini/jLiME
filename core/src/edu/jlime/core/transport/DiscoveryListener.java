package edu.jlime.core.transport;

import java.util.Map;

public interface DiscoveryListener {

	void memberMessage(Address from, String name, Map<String, String> data)
			throws Exception;
}