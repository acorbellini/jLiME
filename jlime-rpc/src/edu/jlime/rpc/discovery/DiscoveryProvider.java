package edu.jlime.rpc.discovery;

import java.util.HashMap;

public interface DiscoveryProvider {

	public abstract void addListener(DiscoveryListener l);

	public abstract void putData(HashMap<String, String> dataMap);
}