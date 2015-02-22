package edu.jlime.graphly.util;

import edu.jlime.jd.ClientNode;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

	public static List<Pair<ClientNode, TLongArrayList>> divide(
			Map<ClientNode, TLongArrayList> div, int max) {
		ArrayList<Pair<ClientNode, TLongArrayList>> ret = new ArrayList<>();
		for (Entry<ClientNode, TLongArrayList> entry : div.entrySet()) {
			ClientNode cli = entry.getKey();
			TLongArrayList list = entry.getValue();
			int divisions = (list.size() - 1) / max;
			if (divisions <= 1) {
				ret.add(Pair.build(cli, list));
			} else
				for (int i = 0; i <= divisions; i++) {
					int begin = i * max;
					TLongArrayList sublist = new TLongArrayList(list.subList(
							begin, Math.min(begin + max, list.size())));
					ret.add(Pair.build(cli, sublist));
				}

		}
		return ret;
	}

}
