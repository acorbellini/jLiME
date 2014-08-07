package edu.jlime.linkprediction;

import edu.jlime.collections.util.IntArrayUtils;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class SimilarityUtil {

	public static Map<Integer, Float> commonneighbours(int[] users,
			final TIntObjectHashMap<int[]> nList, final int[] list)
			throws Exception {
		final Map<Integer, Float> res = Collections
				.synchronizedMap(new HashMap<Integer, Float>());
		ExecutorService exec = Executors.newCachedThreadPool();
		final Semaphore toAdd = new Semaphore(10);
		final Semaphore lock = new Semaphore(-users.length + 1);
		for (final int u : users) {
			toAdd.acquire();
			exec.execute(new Runnable() {

				@Override
				public void run() {
					int intersect = IntArrayUtils.intersectCount(nList.get(u),
							list);
					float union = (float) IntArrayUtils.unionCount(
							nList.get(u), list);
					float val = intersect / union;
					res.put(u, val);
					toAdd.release();
					lock.release();

				}
			});
		}
		lock.acquire();

		if (res.containsValue(Float.NaN))
			System.out.println("RES CONTAINS NOT A NUMBER!! NaN");
		return res;

	}
}
