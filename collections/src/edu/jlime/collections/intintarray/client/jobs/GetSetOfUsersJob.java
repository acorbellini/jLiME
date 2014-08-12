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
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

public class GetSetOfUsersJob implements Job<int[]> {

	private static final long serialVersionUID = 3437379208216701568L;

	private int[] kList;

	private String storeName;

	public GetSetOfUsersJob(int[] k, String name) {
		this.kList = k;
		this.storeName = name;
	}

	ExecutorService exec = Executors.newFixedThreadPool(10);

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
			// Arrays.sort(subset);
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
	public int[] call(JobContext ctx, JobNode peer) throws Exception {
		Logger log = Logger.getLogger(MultiGetJob.class);
		log.info("Obtaining multiple keys (" + kList.length + ") from store");
		TIntHashSet hash = new TIntHashSet();
		Store store = (Store) ctx.get(storeName);
		// List<Future<TIntHashSet>> futures = new ArrayList<>();
		// int listSize = 1000000;
		// int remaining = kList.length;
		// int lists = (int) Math.ceil(kList.length / (double) listSize);
		// for (int i = 0; i < lists; i++) {
		// int init_chunk = i * listSize;
		// int len_chunk = Math.min(remaining, listSize);
		// futures.add(exec.submit(new SubSet(Arrays.copyOfRange(kList,
		// init_chunk, init_chunk + len_chunk), store)));
		// remaining -= listSize;
		// }

		List<byte[]> res = new ArrayList<>(kList.length);
		Arrays.sort(kList);
		for (int u : kList) {
			byte[] valAsBytes = store.load(u);
			if (valAsBytes != null) {
				res.add(valAsBytes);
			}
			// res.put(u, new int[] {});
		}

		for (Iterator<byte[]> iterator = res.iterator(); iterator.hasNext();) {
			byte[] bs = iterator.next();
			hash.addAll(DataTypeUtils.byteArrayToIntArray(bs));
			iterator.remove();
		}

		// exec.shutdown();

		// log.info("Merging results");
		// while (!futures.isEmpty()) {
		// for (Iterator<Future<TIntHashSet>> iterator = futures.iterator();
		// iterator
		// .hasNext();) {
		// Future<TIntHashSet> bs = iterator.next();
		// if (bs.isDone()) {
		// hash.addAll(bs.get());
		// iterator.remove();
		// }
		// }
		// Thread.sleep(1);
		// }

		log.info("Returning result for GetSetOfUsersJob with " + hash.size()
				+ " users.");
		return hash.toArray();
	}
}