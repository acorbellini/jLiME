package edu.jlime.collections.util;

import edu.jlime.client.Client;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;

public class Get {
	public static void main(String[] args) throws Exception {
		Client cli = Client.build(8);
		PersistentIntIntArrayMap map = new PersistentIntIntArrayMap(
				new StoreConfig(StoreType.LEVELDB,
						"/home/acorbellini/TwitterDB", "TwitterLevelDB"),
				cli.getCluster());

		System.out.println(map.get(12).length);
		System.out.println(map.get(-12).length);

		cli.close();
	}
}
