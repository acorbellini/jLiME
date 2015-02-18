package edu.jlime.graphly.util;

import gnu.trove.list.array.TLongArrayList;

import java.util.Arrays;

public class GraphlyUtil {

	public static boolean in(Long v, long[] first) {
		return Arrays.binarySearch(first, v) >= 0;
	}

	public static long[] filter(long[] edges, long[] all) {
		TLongArrayList wrap = TLongArrayList.wrap(edges);
		wrap.retainAll(all);
		return wrap.toArray();
	}

}
