package edu.jlime.collections;

import java.util.Calendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.junit.Test;

import edu.jlime.collections.intintarray.db.LevelDb;
import edu.jlime.collections.intintarray.db.StoreFactory;
import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.StringUtils;
import gnu.trove.map.hash.TIntObjectHashMap;

public class DBTest {

	@Test
	public void dbtest() throws Exception {
		StoreFactory storeFactory = new StoreFactory(StoreType.LEVELDB);
		final LevelDb store = (LevelDb) storeFactory.getStore("testPerf",
				"./testPerfDB");
		int users = 2000000;
		int max_list = 1000;

		for (int i = 0; i < users; i++) {
			int[] list = new int[(int) (Math.random() * max_list + 1)];
			for (int j = 0; j < list.length; j++) {
				list[j] = (int) (Math.random() * users + 1);
			}
			store.store(i, DataTypeUtils.intArrayToByteArray(list));
		}

		final TIntObjectHashMap<int[]> res = new TIntObjectHashMap<>();

		long init = Calendar.getInstance().getTimeInMillis();
		for (int k = 0; k < users; k++) {
			int[] val = DataTypeUtils.byteArrayToIntArray(store.load(k));
			res.put(k, val);
		}
		long end = Calendar.getInstance().getTimeInMillis();

		System.out.println(StringUtils.readableTime(end - init));

		final Semaphore finished = new Semaphore(-users + 1);
		final Semaphore sem = new Semaphore(500);
		final ExecutorService exec = Executors.newFixedThreadPool(500);
		// final ExecutorService exec = Executors.newCachedThreadPool();

		res.clear();

		init = Calendar.getInstance().getTimeInMillis();

		for (int k = 0; k < users; k++) {
			final int u = k;
			try {
				sem.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			exec.execute(new Runnable() {
				@Override
				public void run() {
					int[] val = null;
					try {
						val = DataTypeUtils.byteArrayToIntArray(store.load(u));
					} catch (Exception e) {
						e.printStackTrace();
					}
					synchronized (res) {
						res.put(u, val);
					}
					sem.release();
					finished.release();
				}
			});
		}

		finished.acquire();
		end = Calendar.getInstance().getTimeInMillis();
		System.out.println(StringUtils.readableTime(end - init));

	}
}
