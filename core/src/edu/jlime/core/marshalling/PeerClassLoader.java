package edu.jlime.core.marshalling;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;

public class PeerClassLoader {

	Map<String, byte[]> classDefCache = new ConcurrentHashMap<String, byte[]>();

	Map<Peer, ClientClassLoader> classLoaders = new ConcurrentHashMap<>();

	Logger log = Logger.getLogger(PeerClassLoader.class);

	public void add(Peer classSource, ClientClassLoader loader) {
		classLoaders.put(classSource, loader);
	}

	public ClientClassLoader getCL(Peer addr) {
		return classLoaders.get(addr);
	}

	public void remove(Peer address) {
		classLoaders.remove(address);
	}

}