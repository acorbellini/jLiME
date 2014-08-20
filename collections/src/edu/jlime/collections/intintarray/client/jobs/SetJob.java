package edu.jlime.collections.intintarray.client.jobs;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.Job;
import edu.jlime.util.DataTypeUtils;

public class SetJob implements Job<Boolean> {

	private static final long serialVersionUID = -8524042757588380786L;

	private int k;

	private int[] d;

	private String storeName;

	public SetJob(int k, int[] d, String name) {
		this.k = k;
		this.d = d;
		this.storeName = name;
	}

	@Override
	public Boolean call(JobContext ctx, ClientNode peer) throws Exception {
		final Store store = (Store) ctx.get(storeName);
		store.store(k, DataTypeUtils.intArrayToByteArray(d));
		return true;
	}

}