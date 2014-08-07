package edu.jlime.rpc.discovery;

import java.util.Map;

import edu.jlime.rpc.message.Address;

public interface DiscoveryListener {

	void memberMessage(Address from, Map<String, String> data) throws Exception;
}