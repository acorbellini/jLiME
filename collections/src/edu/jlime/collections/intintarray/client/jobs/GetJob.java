package edu.jlime.collections.intintarray.client.jobs;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.util.DataTypeUtils;

public class GetJob implements Job<int[]> {

	private static final long serialVersionUID = 1635782214398701566L;

	private int k;

	private String storeName;

	public GetJob(int k, String storeName) {
		this.k = k;
		this.storeName = storeName;
	}

	@Override
	public int[] call(JobContext ctx, JobNode peer) throws Exception {
		Store store = (Store) ctx.get(storeName);
		return DataTypeUtils.byteArrayToIntArray(store.load(k));
	}
}
