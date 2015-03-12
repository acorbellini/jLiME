package edu.jlime.collections.intintarray.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.jlime.collections.intintarray.client.jobs.MultiGetJob;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

public class CountListsJob implements Job<TIntIntHashMap> {

	private static final int CACHE = 50000;

	private static final long serialVersionUID = 3437379208216701568L;

	private int[] kList;

	private String storeName;

	public CountListsJob(int[] k, String name) {
		this.kList = k;
		this.storeName = name;
	}

	@Override
	public TIntIntHashMap call(JobContext ctx, ClientNode peer)
			throws Exception {

		final TIntIntHashMap hash = new TIntIntHashMap(50000, 0.8f);

		final Logger log = Logger.getLogger(MultiGetJob.class);

		log.info("Obtaining multiple keys (" + kList.length + ") from store");

		final Store store = (Store) ctx.get(storeName);

		final Semaphore sem = new Semaphore(2);

		ExecutorService exec = Executors.newCachedThreadPool();
		log.info("Loading kList.");
		TIntArrayList curr = new TIntArrayList(CACHE);
		for (int i = 0; i < kList.length; i++) {
			curr.add(kList[i]);
			if (curr.size() > CACHE) {
				execute(hash, store, sem, exec, curr);
				curr = new TIntArrayList(CACHE);
			}

		}
		if (!curr.isEmpty())
			execute(hash, store, sem, exec, curr);

		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		log.info("Loaded kList.");

		return hash;
		// return res;

		// log.info("Loading kList.");
		// List<byte[]> loadAll = store.loadAll(kList);
		// log.info("Loaded kList.");
		// for (final byte[] valAsBytes : loadAll) {
		// if (valAsBytes != null) {
		// int[] intArray = DataTypeUtils
		// .byteArrayToIntArray((byte[]) valAsBytes);
		// for (int b : intArray)
		// hash.adjustOrPutValue(b, 1, 1);
		// }
		//
		// }
		// return hash;
	}

	private void execute(final TIntIntHashMap hash, final Store store,
			final Semaphore sem, ExecutorService exec, final TIntArrayList curr)
			throws InterruptedException {
		sem.acquire();
		exec.execute(new Runnable() {

			@Override
			public void run() {
				TIntIntHashMap current = new TIntIntHashMap();
				TIntIterator it = curr.iterator();
				while (it.hasNext()) {
					int i = it.next();
					try {
						byte[] load = store.load(i);
						if (load != null) {
							int[] list = DataTypeUtils
									.byteArrayToIntArray(load);
							for (int b : list) {
								current.adjustOrPutValue(b, 1, 1);
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				synchronized (hash) {
					TIntIntIterator itcurr = current.iterator();
					while (itcurr.hasNext()) {
						itcurr.advance();
						int value = itcurr.value();
						hash.adjustOrPutValue(itcurr.key(), value, value);
					}
				}
				sem.release();

			}
		});
	}

	public static TIntIntHashMap fromBytes(byte[] bytes) {
		TIntIntHashMap res = new TIntIntHashMap();
		ByteBuffer reader = new ByteBuffer(bytes);
		int[] keys = DataTypeUtils.byteArrayToIntArray(reader.getByteArray());
		int[] values = DataTypeUtils.byteArrayToIntArray(reader.getByteArray());
		for (int i = 0; i < keys.length; i++) {
			res.put(keys[i], values[i]);
		}
		return res;
	}

}
