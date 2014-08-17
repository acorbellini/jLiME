package edu.jlime.collections.intintarray.client;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.client.jobs.MultiGetJob;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.util.Buffer;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.RingQueue;
import gnu.trove.map.hash.TIntIntHashMap;

public class CountListsJob implements Job<TIntIntHashMap> {

	private static final long serialVersionUID = 3437379208216701568L;

	private int[] kList;

	private String storeName;

	public CountListsJob(int[] k, String name) {
		this.kList = k;
		this.storeName = name;
	}

	private static class SubCount implements Callable<TIntIntHashMap> {

		private int[] subset;
		private Store store;

		public SubCount(int[] subset, Store store) {
			this.subset = subset;
			this.store = store;
		}

		@Override
		public TIntIntHashMap call() throws Exception {
			TIntIntHashMap hash = new TIntIntHashMap();
			Arrays.sort(subset);
			System.out.println("Obtaining a subset of " + subset.length);
			for (int u : subset) {
				byte[] valAsBytes = store.load(u);
				if (valAsBytes != null) {
					for (int i : DataTypeUtils.byteArrayToIntArray(valAsBytes))
						hash.adjustOrPutValue(i, 1, 1);
				}
			}
			System.out.println("Finished obtaining a subset of "
					+ subset.length);
			return hash;
		}
	}

	@Override
	public TIntIntHashMap call(JobContext ctx, JobNode peer) throws Exception {
		ExecutorService exec = Executors.newCachedThreadPool();
		final Logger log = Logger.getLogger(MultiGetJob.class);
		log.info("Obtaining multiple keys (" + kList.length + ") from store");

		Store store = (Store) ctx.get(storeName);

		final RingQueue queue = new RingQueue(2 * 4 * 1024);
		Future<TIntIntHashMap> fut = exec
				.submit(new Callable<TIntIntHashMap>() {

					@Override
					public TIntIntHashMap call() throws Exception {
						TIntIntHashMap hash = new TIntIntHashMap();
						while (true) {
							Object[] vals = queue.take();
							for (Object bs : vals) {
								if (bs == null) {
									log.info("Returning result for CountListsJob with "
											+ hash.size() + " users.");
									return hash;
								}

								int[] intArray = DataTypeUtils
										.byteArrayToIntArray((byte[]) bs);
								for (int b : intArray)
									hash.adjustOrPutValue(b, 1, 1);
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
		for (int u : sorted) {
			byte[] valAsBytes = store.load(u);
			if (valAsBytes != null)
				queue.put(valAsBytes);
		}
		log.info("Finished loading from store");
		queue.put(null);

		return fut.get();
	}

	public static TIntIntHashMap fromBytes(byte[] bytes) {
		TIntIntHashMap res = new TIntIntHashMap();
		Buffer reader = new ByteBuffer(bytes);
		int[] keys = DataTypeUtils.byteArrayToIntArray(reader.getByteArray());
		int[] values = DataTypeUtils.byteArrayToIntArray(reader.getByteArray());
		for (int i = 0; i < keys.length; i++) {
			res.put(keys[i], values[i]);
		}
		return res;
	}

}
