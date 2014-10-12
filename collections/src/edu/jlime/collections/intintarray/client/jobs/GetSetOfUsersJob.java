package edu.jlime.collections.intintarray.client.jobs;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TIntHashSet;

public class GetSetOfUsersJob implements Job<TIntHashSet> {

	private static final long serialVersionUID = 3437379208216701568L;

	private int[] kList;

	private String storeName;

	public GetSetOfUsersJob(int[] k, String name) {
		this.kList = k;
		this.storeName = name;
	}

	@Override
	public TIntHashSet call(JobContext ctx, ClientNode peer) throws Exception {
		final Logger log = Logger.getLogger(MultiGetJob.class);
		log.info("Obtaining multiple keys (" + kList.length + ") from store");
		final Store store = (Store) ctx.get(storeName);
		final TIntHashSet hash = new TIntHashSet(1024, 0.8f);

		final Semaphore sem = new Semaphore(2);

		ExecutorService exec = Executors.newCachedThreadPool();
		log.info("Loading kList.");
		TIntArrayList curr = new TIntArrayList(1000);
		for (int i = 0; i < kList.length; i++) {
			curr.add(kList[i]);
			if (curr.size() == 5000) {
				execute(store, hash, sem, exec, curr);
				curr = new TIntArrayList(1000);
			}
		}
		if (!curr.isEmpty())
			execute(store, hash, sem, exec, curr);

		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		log.info("Loaded kList.");

		return hash;
	}

	private void execute(final Store store, final TIntHashSet hash,
			final Semaphore sem, ExecutorService exec, final TIntArrayList curr)
			throws InterruptedException {
		sem.acquire();
		exec.execute(new Runnable() {

			@Override
			public void run() {
				List<int[]> toAdd = new LinkedList<>();
				TIntIterator it = curr.iterator();
				while (it.hasNext()) {
					int i = it.next();
					try {
						byte[] load = store.load(i);
						if (load != null)
							toAdd.add(DataTypeUtils.byteArrayToIntArray(load));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				synchronized (hash) {
					for (int[] valAsBytes : toAdd) {
						for (int b : valAsBytes)
							hash.add(b);
					}
				}
				sem.release();

			}
		});
	}
}