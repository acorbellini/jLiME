package edu.jlime.collections.intintarray.client.jobs;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.RingQueue;
import gnu.trove.set.hash.TIntHashSet;

public class GetSetOfUsersJob implements Job<int[]> {

	private static final long serialVersionUID = 3437379208216701568L;

	private int[] kList;

	private String storeName;

	public GetSetOfUsersJob(int[] k, String name) {
		this.kList = k;
		this.storeName = name;
	}

	private static class SubSet implements Callable<TIntHashSet> {

		private int[] subset;
		private Store store;

		public SubSet(int[] subset, Store store) {
			this.subset = subset;
			this.store = store;
		}

		@Override
		public TIntHashSet call() throws Exception {
			TIntHashSet hash = new TIntHashSet();
			Arrays.sort(subset);
			for (int u : subset) {
				byte[] valAsBytes = store.load(u);
				if (valAsBytes != null)
					hash.addAll(DataTypeUtils.byteArrayToIntArray(valAsBytes));
			}
			return hash;
		}
	}

	@Override
	public int[] call(JobContext ctx, JobNode peer) throws Exception {
		ExecutorService exec = Executors.newCachedThreadPool();
		final Logger log = Logger.getLogger(MultiGetJob.class);
		log.info("Obtaining multiple keys (" + kList.length + ") from store");
		Store store = (Store) ctx.get(storeName);

		final RingQueue queue = new RingQueue(2 * 4 * 1024);
		Future<TIntHashSet> fut = exec.submit(new Callable<TIntHashSet>() {
			@Override
			public TIntHashSet call() throws Exception {
				TIntHashSet hash = new TIntHashSet();
				while (true) {
					Object[] vals = queue.take();
					for (Object bs : vals) {
						if (bs == null) {
							log.info("Returning result for GetSetOfUsersJob with "
									+ hash.size() + " users.");
							return hash;
						}
						int[] byteArrayToIntArray = DataTypeUtils
								.byteArrayToIntArray((byte[]) bs);
						hash.addAll(byteArrayToIntArray);
					}
				}
			}
		});
		exec.shutdown();

		log.info("Sorting kList");
		Integer[] sorted = ArrayUtils.toObject(kList);

		Arrays.sort(sorted, new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				byte[] b1 = DataTypeUtils.intToByteArray(o1);
				byte[] b2 = DataTypeUtils.intToByteArray(o2);
				for (int i = 0; i < 4; i++) {
					int comp = Byte.compare(b1[i], b2[i]);
					if (comp != 0)
						return comp;
				}
				return 0;
			}
		});
		log.info("Sorted kList");
		for (int k : sorted) {
			byte[] valAsBytes = store.load(k);
			if (valAsBytes != null) {
				queue.put(valAsBytes);
			}
		}
		log.info("Finished loading from store");
		queue.put(null);

		return fut.get().toArray();
	}
}