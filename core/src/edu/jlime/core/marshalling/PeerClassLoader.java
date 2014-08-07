package edu.jlime.core.marshalling;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class PeerClassLoader {

	Map<String, byte[]> classDefCache = new ConcurrentHashMap<String, byte[]>();

	Map<String, ClientClassLoader> classLoaders = new ConcurrentHashMap<String, ClientClassLoader>();

	Logger log = Logger.getLogger(PeerClassLoader.class);

	public void add(String classSource, ClientClassLoader loader) {
		classLoaders.put(classSource, loader);
	}

	public ClientClassLoader getCL(String addr) {
		return classLoaders.get(addr);
	}

	public void remove(String address) {
		classLoaders.remove(address);
	}

}