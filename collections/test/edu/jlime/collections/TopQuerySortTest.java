package edu.jlime.collections;

import edu.jlime.collections.adjacencygraph.query.TopQuery;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.log4j.Logger;

public class TopQuerySortTest {
	public static void main(String[] args) {
		int top = 10;
		final TIntIntHashMap countres = new TIntIntHashMap();
		countres.put(1, 100);
		countres.put(2, 15);
		countres.put(3, 1);
		countres.put(4, 3);
		countres.put(5, 140);
		countres.put(6, 1123);
		countres.put(7, 2);
		countres.put(8, 86);
		countres.put(9, 12);
		countres.put(10, 3);
		countres.put(11, 1);
		countres.put(12, 1301);
		countres.put(13, 111);
		countres.put(14, 161);

		Logger logger = Logger.getLogger(TopQuery.class);
		logger.info("Obtaining " + top + " elements from query.");
		TreeSet<Integer> finalRes = new TreeSet<Integer>(
				new Comparator<Integer>() {

					@Override
					public int compare(Integer o1, Integer o2) {
						return Integer.compare(countres.get(o1),
								countres.get(o2));
					}
				});
		TIntIntIterator it = countres.iterator();
		while (it.hasNext()) {
			it.advance();
			int k = it.key();
			int v = it.value();
			if (finalRes.size() < top)
				finalRes.add(k);
			else {
				if (v > countres.get(finalRes.first())) {
					finalRes.remove(finalRes.first());
					finalRes.add(k);
				}
			}
		}

		logger.info("Finished obtaining " + top + " elements from query.");
		List<int[]> res = new ArrayList<>();
		for (Integer k : finalRes) {
			res.add(new int[] { k, countres.get(k) });
		}
		Collections.sort(res, new Comparator<int[]>() {

			@Override
			public int compare(int[] o1, int[] o2) {
				return new Integer(o1[1]).compareTo(new Integer(o2[1])) * -1;
			}

		});
		for (int[] is : res) {
			System.out.println(is[0] + "=" + is[1]);
		}

	}
}
