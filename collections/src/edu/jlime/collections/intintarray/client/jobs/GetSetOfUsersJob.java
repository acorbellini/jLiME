package edu.jlime.collections.intintarray.client.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.RingQueue;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

public class GetSetOfUsersJob implements Job<TIntHashSet> {

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
				if (valAsBytes != null) {
					hash.addAll(DataTypeUtils.byteArrayToIntArray(valAsBytes));
				}
				// res.put(u, new int[] {});
			}
			return hash;
		}
	}

	@Override
	public TIntHashSet call(JobContext ctx, JobNode peer) throws Exception {
		ExecutorService exec = Executors.newCachedThreadPool();
		final Logger log = Logger.getLogger(MultiGetJob.class);
		log.info("Obtaining multiple keys (" + kList.length + ") from store");
		Store store = (Store) ctx.get(storeName);

		// TIntHashSet hash = null;
		// Arrays.sort(kList);
		// for (int i : kList) {
		// byte[] load = store.load(i);
		// if (load != null) {
		// int[] byteArrayToIntArray = DataTypeUtils
		// .byteArrayToIntArray(load);
		// if (hash == null)
		// hash = new TIntHashSet(byteArrayToIntArray.length);
		//
		// hash.addAll(byteArrayToIntArray);
		// }
		// }
		// log.info("Returning result for GetSetOfUsersJob with "
		// + (hash != null ? hash.size() : null) + " users.");
		// return hash;

		final RingQueue queue = new RingQueue();
		Future<TIntHashSet> fut = exec.submit(new Callable<TIntHashSet>() {
			@Override
			public TIntHashSet call() throws Exception {
				TIntHashSet hash = null;
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
						if (hash == null)
							hash = new TIntHashSet(byteArrayToIntArray.length);
						hash.addAll(byteArrayToIntArray);
					}
				}
			}
		});
		exec.shutdown();
		Arrays.sort(kList);
		// List<byte[]> collected = store.loadAll(kList);
		for (int u : kList) {
			byte[] valAsBytes = store.load(u);
			if (valAsBytes != null)
				queue.put(valAsBytes);
			// res.put(u, new int[] {});
		}
		queue.put(null);

		return fut.get();
	}
}