package edu.jlime.collections.intintarray.client.jobs;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.Job;

public class PersistentIntIntArrayInitJob implements Job<Boolean> {

	private static final long serialVersionUID = -759050256499091466L;

	private StoreConfig storeConfig;

	private String storeName;

	public PersistentIntIntArrayInitJob(String store, StoreConfig storeConfig) {
		this.storeConfig = storeConfig;
		this.storeName = store;
	}

	@Override
	public Boolean call(JobContext ctx, ClientNode peer) throws Exception {
		Logger log = Logger.getLogger(PersistentIntIntArrayInitJob.class);
		if (log.isDebugEnabled())
			log.debug("Initializing PersistentIntIntMap requested from " + peer);
		synchronized (ctx) {
			if (ctx.get(storeName) == null)
				ctx.put(storeName, Store.init(storeConfig));
			if (log.isDebugEnabled())
				log.debug("Returning from init job of persistent hash, requested from "
						+ peer);
			return true;
		}

	}
}
