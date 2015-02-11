package edu.jlime.graphly.util;

import java.util.Arrays;

public class GraphlyUtil {

	public static boolean in(Long v, long[] first) {
		return Arrays.binarySearch(first, v) >= 0;
	}

}
