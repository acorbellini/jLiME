package edu.jlime.core.transport;

import java.util.Map;

public interface DiscoveryProvider {

	public abstract void addListener(DiscoveryListener l);

	public abstract void putData(Map<String, String> dataMap);

	public abstract Object getAddresses();
}