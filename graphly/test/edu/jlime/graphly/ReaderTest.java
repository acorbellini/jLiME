package edu.jlime.graphly;

import edu.jlime.graphly.util.GraphlySintetic;
import edu.jlime.graphly.util.Pair;
import gnu.trove.list.array.TLongArrayList;

import java.io.IOException;
import java.util.Iterator;

public class ReaderTest {
	public static void main(String[] args) throws NumberFormatException,
			IOException {
		long[] edges = new long[] { 2, 4, 5, 6, 7, 11, 23, 5, 1 };
		TLongArrayList ret = new TLongArrayList();
		int max_edges = 5;
		int in = 0;
		for (in = 0; in < edges.length && ret.size() < max_edges; in++) {
			int rn = edges.length - in;
			int rm = max_edges - ret.size();
			if (Math.random() * rn < rm)
				/* Take it */
				ret.add(edges[in]); /*
									 * +1 since your range begins from 1
									 */
		}
		System.out.println(ret);
		Iterator<Pair<Long, long[]>> adj = GraphlySintetic.read("sinth.g.in",
				"\t");
		System.out.println(adj.next());
	}
}
