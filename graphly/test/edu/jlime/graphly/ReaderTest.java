package edu.jlime.graphly;

import java.io.IOException;
import java.util.Iterator;

import edu.jlime.graphly.util.GraphlySintetic;
import edu.jlime.graphly.util.Pair;

public class ReaderTest {
	public static void main(String[] args) throws NumberFormatException,
			IOException {
		Iterator<Pair<Long, long[]>> adj = GraphlySintetic.read("sinth.g.in",
				"\t");
		System.out.println(adj.next());
	}
}
