package edu.jlime.collections.hash;

import edu.jlime.util.StringUtils;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntIntProcedure;

import java.util.Arrays;
import java.util.Calendar;

import org.junit.Test;

public class HashTest {

	@Test
	public void hashTest() throws Exception {
		int users = 100;
		//
		long init = Calendar.getInstance().getTimeInMillis();

		TIntIntHashMap map = new TIntIntHashMap();
		for (int i = 0; i < users; i++) {
			// map.put((int) (Math.random() * users + 1), (int) (Math.random() *
			// users + 1));
			map.put(i, (int) (Math.random() * users + 1));
		}
		long end = Calendar.getInstance().getTimeInMillis();
		System.out.println(StringUtils.readableTime(end - init));

		final SimpleIntIntHash hash = new SimpleIntIntHash();

		long init2 = Calendar.getInstance().getTimeInMillis();
		map.forEachEntry(new TIntIntProcedure() {
			@Override
			public boolean execute(int k, int v) {
				hash.put(k, v);
				return true;
			}
		});
		long end2 = Calendar.getInstance().getTimeInMillis();
		map.forEachEntry(new TIntIntProcedure() {
			@Override
			public boolean execute(int k, int v) {
				try {
					assert (hash.get(k) == v);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return true;
			}
		});

		System.out.println(Arrays.toString(hash.keys()));

		System.out.println(hash.keys().length);
		System.out.println(StringUtils.readableTime(end2 - init2));
		//
		// DoubleHash dhash = new DoubleHash();
		// long init3 = Calendar.getInstance().getTimeInMillis();
		// for (int i = 0; i < users; i++) {
		// // hash.put(i, map.get(i));
		// dhash.put(i, (int) (Math.random() * users + 1));
		// }
		// long end3 = Calendar.getInstance().getTimeInMillis();
		//
		// System.out.println(StringUtils.readableTime(end3 - init3));
		//
		// for (int i = 0; i < users; i++) {
		// assert (hash.get(i) == map.get(i));
		// }

		// hash.putOrAdd(1, 5);
		// hash.putOrAdd(1, 5);
		// hash.putOrAdd(1, 5);
		// hash.putOrAdd(1, 5);
		//
		// hash.putOrAdd(2, 1);
		// hash.putOrAdd(2, 2);
		// hash.putOrAdd(2, 3);
		// hash.putOrAdd(2, 4);
		//
		// hash.putOrAdd(3, 15);
		// hash.putOrAdd(3, 15);
		//
		// hash.putOrAdd(4, 20);
		// hash.putOrAdd(4, 20);
		//
		// hash.putOrAdd(5, 30);
		// hash.putOrAdd(5, 60);
		//
		// hash.putOrAdd(7, 100);
		// hash.putOrAdd(7, 10);
		//
		// for (int[] is : hash) {
		// System.out.println(is[0] + "," + is[1]);
		// }
	}
}
