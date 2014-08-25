package edu.jlime.collections.intintarray.client;

import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class PersistentIntIntArrayCloseJob implements Job<Boolean> {

	private static final long serialVersionUID = -6936268433598111007L;

	private String storeName;

	public PersistentIntIntArrayCloseJob(String store) {
		this.storeName = store;
	}

	@Override
	public Boolean call(JobContext ctx, ClientNode peer) throws Exception {
		System.out.println("Removing DKVS Store.");
		Store store = (Store) ctx.remove(storeName);
		if (store != null)
			store.close();
		return true;
	}
}
