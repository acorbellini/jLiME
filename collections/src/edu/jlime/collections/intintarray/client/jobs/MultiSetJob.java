package edu.jlime.collections.intintarray.client.jobs;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TIntObjectHashMap;

public class MultiSetJob implements Job<Boolean> {

	private static final long serialVersionUID = -1371919252646356536L;

	private TIntObjectHashMap<int[]> kList;

	private String storeName;

	public MultiSetJob(TIntObjectHashMap<int[]> kList, String name) {
		this.kList = kList;
		this.storeName = name;
	}

	@Override
	public Boolean call(JobContext ctx, JobNode peer) throws Exception {
		final Store store = (Store) ctx.get(storeName);
		TIntObjectIterator<int[]> it = kList.iterator();
		while (it.hasNext()) {
			it.advance();
			store.store(it.key(), DataTypeUtils.intArrayToByteArray(it.value()));
		}
		return true;
	}
}
