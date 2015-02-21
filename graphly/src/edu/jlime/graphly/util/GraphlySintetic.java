package edu.jlime.graphly.util;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class GraphlySintetic {
	private TLongObjectHashMap<TLongHashSet> out = new TLongObjectHashMap<TLongHashSet>();

	private TLongObjectHashMap<TLongHashSet> in = new TLongObjectHashMap<TLongHashSet>();

	public GraphlySintetic(int v, int e, float c) {
		for (int i = 0; i < v; i++) {
			TLongHashSet map = createEdges(v, e, c);
			out.put(i, map);
			TLongIterator it = map.iterator();
			while (it.hasNext()) {
				long vid = it.next();
				TLongHashSet set = in.get(vid);
				if (set == null) {
					set = new TLongHashSet();
					in.put(vid, set);
				}
				set.add(i);
			}

		}
	}

	public static void main(String[] args) throws IOException {
		String fName = args[0];
		int vertices = Integer.valueOf(args[1]);
		int edges = Integer.valueOf(args[2]);
		float connectivity = Float.valueOf(args[3]);

		GraphlySintetic sint = new GraphlySintetic(vertices, edges,
				connectivity);
		sint.writeTo(fName, args[4]);

	}

	private void writeTo(String fName, String sep) throws IOException {
		FileWriter outwriter = new FileWriter(new File(fName + ".out"));
		long[] keys = out.keys();
		Arrays.sort(keys);
		for (long l : keys) {
			TLongIterator it = out.get(l).iterator();
			while (it.hasNext()) {
				long to = it.next();
				outwriter.write(l + sep + to + "\n");
			}
		}
		outwriter.close();

		FileWriter inwriter = new FileWriter(new File(fName + ".in"));
		long[] inkeys = in.keys();
		Arrays.sort(inkeys);
		for (long l : inkeys) {
			TLongIterator it = in.get(l).iterator();
			while (it.hasNext()) {
				long to = it.next();
				inwriter.write(l + sep + to + "\n");
			}
		}
		inwriter.close();
	}

	private TLongHashSet createEdges(int vertices, int edges, float connectivity) {
		TLongHashSet ret = new TLongHashSet();
		for (int i = 0; i < edges; i++) {
			double r = Math.random();
			if (r <= connectivity) {
				ret.add((long) (Math.random() * vertices));
			}
		}
		return ret;
	}

	public static Iterator<Pair<Long, long[]>> read(String file, String sep)
			throws NumberFormatException, IOException {
		return new GraphReader(file, sep);
	}
}
