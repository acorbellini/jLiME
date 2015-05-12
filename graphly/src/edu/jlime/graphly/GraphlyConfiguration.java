package edu.jlime.graphly;

import edu.jlime.rpc.Configuration;

public class GraphlyConfiguration {
	Configuration config;

	public int cacheLength;

	public boolean persistfloats;

	public float cacheSize;

	public String edgeCacheType;

	public int storePool;

	public int storeCache;

	public GraphlyConfiguration() {
		this(new Configuration());
	}

	public GraphlyConfiguration(Configuration newConfig) {
		this.config = newConfig;
		this.persistfloats = newConfig.getBoolean(
				"graphly.store.persistfloats", false);

		this.edgeCacheType = newConfig.getString("graphly.store.edgecachetype",
				"fixed-size");// mem-based, no-cache

		this.cacheLength = newConfig.getInt("graphly.store.cachelength", 1000);
		this.cacheSize = newConfig.getFloat("graphly.store.cachesize", 0.1f);

		this.storePool = newConfig.getInt("graphly.store.internalcache",
				400 * 1024 * 1024);

		this.storeCache = newConfig.getInt("graphly.store.internalpool",
				512 * 1024);

	}

}
