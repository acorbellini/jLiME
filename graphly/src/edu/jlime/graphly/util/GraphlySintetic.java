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

import org.apache.commons.lang3.tuple.Pair;

public class GraphlySintetic {
	private TLongObjectHashMap<TLongHashSet> g;

	public GraphlySintetic(int v, int e, float c) {
		this.g = new TLongObjectHashMap<TLongHashSet>();
		for (int i = 0; i < v; i++) {
			TLongHashSet map = createEdges(v, e, c);
			g.put(i, map);
			TLongIterator it = map.iterator();
			while (it.hasNext()) {
				long vid = it.next();
				TLongHashSet set = g.get(-vid - 1);
				if (set == null) {
					set = new TLongHashSet();
					g.put(-vid - 1, set);
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
		sint.writeTo(fName);

	}

	private void writeTo(String fName) throws IOException {
		FileWriter writer = new FileWriter(new File(fName));
		long[] keys = g.keys();
		Arrays.sort(keys);
		for (long l : keys) {
			TLongIterator it = g.get(l).iterator();
			while (it.hasNext()) {
				long to = it.next();
				writer.write(l + "," + to + "\n");
			}
		}
		writer.close();
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

	public static Iterator<Pair<Long, long[]>> read(String string)
			throws NumberFormatException, IOException {
		final BufferedReader reader = new BufferedReader(new FileReader(
				new File(string)));
		return new Iterator<Pair<Long, long[]>>() {
			private String nextLine;

			@Override
			public Pair<Long, long[]> next() {
				TLongArrayList map = new TLongArrayList();
				Long curr = null;
				boolean done = false;
				try {
					while ((nextLine != null || reader.ready()) && !done) {
						String line = "";
						if (nextLine != null) {
							line = nextLine;
							nextLine = null;
						} else
							line = reader.readLine();

						long k = Long.valueOf(line.substring(0,
								line.indexOf(",")));

						if (curr == null)
							curr = k;

						if (curr != k) {
							nextLine = line;
							done = true;
						} else {
							long v = Long.valueOf(line.substring(
									line.indexOf(",") + 1, line.length()));
							map.add(v);
						}
					}

				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

				return Pair.of(curr, map.toArray());
			}

			@Override
			public boolean hasNext() {
				try {
					return nextLine != null || reader.ready();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return false;
			}
		};
	}
}
