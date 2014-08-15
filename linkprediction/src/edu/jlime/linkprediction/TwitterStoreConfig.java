package edu.jlime.linkprediction;

import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;

public class TwitterStoreConfig {

	public static StoreConfig getConfig() {
		return new StoreConfig(StoreType.LEVELDB,
				"/home/acorbellini/TwitterDBV2", "TwitterLevelDB");
	}

}
