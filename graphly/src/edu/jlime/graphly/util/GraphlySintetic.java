package edu.jlime.graphly.util;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class GraphlySintetic {
	private TLongObjectHashMap<TLongHashSet> out = new TLongObjectHashMap<TLongHashSet>();

	private TLongObjectHashMap<TLongHashSet> in = new TLongObjectHashMap<TLongHashSet>();

	public GraphlySintetic(int v, int e, float c, long[] ls) {
		for (int currVertex = 0; currVertex < v; currVertex++) {
			createVertex(v, ls, e, c, currVertex);
		}

		for (long currVertex : ls) {
			createVertex(v, ls, e, c, currVertex);
		}
	}

	private void createVertex(int v, long[] ul, int e, float c, long currVertex) {
		TLongHashSet map = createEdges(v, e, c, ul);
		out.put(currVertex, map);
		TLongIterator it = map.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			TLongHashSet set = in.get(vid);
			if (set == null) {
				set = new TLongHashSet();
				in.put(vid, set);
			}
			set.add(currVertex);
		}
	}

	public static void main(String[] args) throws IOException {
		String fName = args[0];
		int vertices = Integer.valueOf(args[1]);
		int edges = Integer.valueOf(args[2]);
		float connectivity = Float.valueOf(args[3]);

		BufferedReader reader = new BufferedReader(new FileReader(new File(
				args[5])));
		TLongArrayList vids = new TLongArrayList();
		while (reader.ready()) {
			long vid = Long.valueOf(reader.readLine().trim());
			vids.add(vid);
		}
		reader.close();

		GraphlySintetic sint = new GraphlySintetic(vertices, edges,
				connectivity, vids.toArray());
		sint.writeTo(fName, args[4]);

	}

	private void writeTo(String fName, String sep) throws IOException {
		FileWriter outwriter = new FileWriter(new File(fName + ".out"));
		long[] keys = out.keys();
		Arrays.sort(keys);
		for (long l : keys) {
			long[] edges = out.get(l).toArray();
			Arrays.sort(edges);
			for (long to : edges) {
				outwriter.write(l + sep + to + "\n");
			}
		}
		outwriter.close();

		FileWriter inwriter = new FileWriter(new File(fName + ".in"));
		long[] inkeys = in.keys();
		Arrays.sort(inkeys);
		for (long l : inkeys) {
			long[] edges = in.get(l).toArray();
			Arrays.sort(edges);
			for (long to : edges) {
				inwriter.write(l + sep + to + "\n");
			}
		}
		inwriter.close();
	}

	private TLongHashSet createEdges(int vertices, int edges,
			float connectivity, long[] ul) {
		TLongHashSet ret = new TLongHashSet();
		for (int i = 0; i < edges; i++) {
			double r = Math.random();
			if (r <= connectivity) {
				double src = Math.random();
				if (src >= 0.5)
					ret.add(ul[(int) (Math.random() * ul.length)]);
				else
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
