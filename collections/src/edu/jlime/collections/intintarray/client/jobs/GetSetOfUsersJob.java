package edu.jlime.collections.intintarray.client.jobs;

import java.util.Arrays;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.set.hash.TIntHashSet;

public class GetSetOfUsersJob implements Job<int[]> {

	private static final long serialVersionUID = 3437379208216701568L;

	private int[] kList;

	private String storeName;

	public GetSetOfUsersJob(int[] k, String name) {
		this.kList = k;
		this.storeName = name;
	}

	@Override
	public int[] call(JobContext ctx, JobNode peer) throws Exception {
		Logger log = Logger.getLogger(MultiGetJob.class);
		log.info("Obtaining multiple keys (" + kList.length + ") from store");
		TIntHashSet hash = new TIntHashSet();
		Store store = (Store) ctx.get(storeName);
		for (int u : kList) {
			byte[] valAsBytes = store.load(u);
			if (valAsBytes != null) {
				int[] obtained = DataTypeUtils.byteArrayToIntArray(valAsBytes);
				hash.addAll(obtained);
			}
			// res.put(u, new int[] {});
		}
		log.info("Returning result for GetSetOfUsersJob with " + hash.size()
				+ " users.");
		return hash.toArray();
	}
}