package edu.jlime.graphly;

import edu.jlime.rpc.Configuration;

public class GraphlyConfig {
	Configuration config;

	public int cacheLength;

	public boolean persistfloats;

	public float cacheSize;

	public String edgeCacheType;

	public int storePool;

	public int storeCache;

	public boolean persistObjects;

	public GraphlyConfig() {
		this(new Configuration());
	}

	public GraphlyConfig(Configuration newConfig) {
		this.config = newConfig;
		this.persistfloats = newConfig.getBoolean("graphly.store.persistfloats", false);
		this.persistObjects = newConfig.getBoolean("graphly.store.persistStrings", true);
		this.edgeCacheType = newConfig.getString("graphly.store.edgecachetype", "no-cache");// fixed-size,
																							// mem-based,
																							// no-cache

		this.cacheLength = newConfig.getInt("graphly.store.cachelength", 50);
		this.cacheSize = newConfig.getFloat("graphly.store.cachesize", 0.1f);

		this.storePool = newConfig.getInt("graphly.store.internalcache", 100 * 1024 * 1024);

		this.storeCache = newConfig.getInt("graphly.store.internalpool", 10 * 1024 * 1024);

	}

}
