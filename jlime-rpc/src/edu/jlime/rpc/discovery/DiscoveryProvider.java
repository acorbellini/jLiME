package edu.jlime.rpc.discovery;

import java.util.Map;

public interface DiscoveryProvider {

	public abstract void addListener(DiscoveryListener l);

	public abstract void putData(Map<String, String> dataMap);
}