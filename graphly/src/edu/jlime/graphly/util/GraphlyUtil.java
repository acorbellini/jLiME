package edu.jlime.graphly.util;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.util.Arrays;

public class GraphlyUtil {

	public static boolean in(Long v, long[] first) {
		return Arrays.binarySearch(first, v) >= 0;
	}

	public static long[] filter(long[] edges, long[] all) {
		try {
			TLongHashSet set = new TLongHashSet(all);
			TLongArrayList wrap = new TLongArrayList(edges);

			TLongIterator it = wrap.iterator();
			while (it.hasNext()) {
				long curr = it.next();
				if (!set.contains(curr))
					it.remove();
			}

			return wrap.toArray();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new long[] {};
	}

}
