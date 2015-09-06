package edu.jlime.graphly.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.jlime.jd.ClientNode;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class GraphlyUtil {

	public static boolean in(Long v, long[] first) {
		return Arrays.binarySearch(first, v) >= 0;
	}

	public static long[] filter(long[] A, long[] B) {
		// try {
		// TLongHashSet set = new TLongHashSet(all);
		// TLongArrayList wrap = new TLongArrayList(edges);
		//
		// TLongIterator it = wrap.iterator();
		// while (it.hasNext()) {
		// long curr = it.next();
		// if (!set.contains(curr))
		// it.remove();
		// }
		//
		// return wrap.toArray();
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// return new long[] {};

		TLongHashSet intersect = new TLongHashSet();
		int i = 0;
		int j = 0;
		while (i < A.length && j < B.length) {
			long a = A[i];
			long b = B[j];
			if (a == b) {
				intersect.add(a);
				i++;
				j++;
			} else if (a < b) {
				i++;
			} else {
				j++;
			}
		}
		long[] toRet = intersect.toArray();
		Arrays.sort(toRet);
		return toRet;
	}

	public static List<Pair<ClientNode, TLongArrayList>> divide(
			Map<ClientNode, TLongArrayList> div, int num) {
		ArrayList<Pair<ClientNode, TLongArrayList>> ret = new ArrayList<>();
		for (Entry<ClientNode, TLongArrayList> entry : div.entrySet()) {
			ClientNode cli = entry.getKey();
			TLongArrayList list = entry.getValue();
			int perjob = (int) Math.ceil(list.size() / (double) num);
			for (int i = 0; i < list.size(); i += perjob) {

				TLongArrayList sublist = new TLongArrayList();
				for (int j = 0; j < perjob && (i + j < list.size()); j++) {
					int offset = Math.min(i + j, list.size() - 1);
					sublist.add(list.get(offset));
				}
				ret.add(Pair.build(cli, sublist));
			}

		}
		return ret;
	}

}
