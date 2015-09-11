package edu.jlime.graphly.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class GraphlySintetic {
	// private TLongObjectHashMap<TLongHashSet> out = new
	// TLongObjectHashMap<TLongHashSet>();

	private static final int MAX_THREADS = 16;

	private ConcurrentHashMap<Long, TLongHashSet> in = new ConcurrentHashMap<Long, TLongHashSet>();

	private String fName;

	private BufferedWriter outwriter;

	private String sep;

	private int v;

	private int e;

	private float c;

	private TLongArrayList ls;

	public GraphlySintetic(String fName, String sep, int v, int e, float c, long[] ls) throws IOException {
		this.fName = fName;
		this.sep = sep;
		this.outwriter = new BufferedWriter(new FileWriter(new File(fName + ".out")));
		this.v = v;
		this.e = e;
		this.c = c;
		this.ls = new TLongArrayList(ls);
	}

	private void createVertex(long currVertex) throws IOException {
		TLongHashSet map = createEdges(v);

		long[] edges = map.toArray();
		Arrays.sort(edges);
		synchronized (outwriter) {
			for (long to : edges) {
				outwriter.write(currVertex + sep + to + "\n");
			}
		}

		TLongIterator it = map.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			TLongHashSet set = in.get(vid);
			if (set == null) {
				synchronized (in) {
					set = in.get(vid);
					if (set == null) {
						set = new TLongHashSet();
						in.put(vid, set);
					}
				}

			}
			synchronized (set) {
				set.add(currVertex);
			}
		}
	}

	public static void main(String[] args) throws IOException {
		String fName = args[0];
		int vertices = Integer.valueOf(args[1]);
		int edges = Integer.valueOf(args[2]);
		float connectivity = Float.valueOf(args[3]);
		String sep = args[4];

		TLongArrayList vids = new TLongArrayList();
		if (args.length > 5) {
			BufferedReader reader = new BufferedReader(new FileReader(new File(args[5])));

			while (reader.ready()) {
				long vid = Long.valueOf(reader.readLine().trim());
				vids.add(vid);
			}
			reader.close();
		}

		GraphlySintetic sint = new GraphlySintetic(fName, sep, vertices, edges, connectivity, vids.toArray());
		sint.create();
	}

	private void create() throws IOException {
		ExecutorService exec = Executors.newFixedThreadPool(MAX_THREADS);
		final Semaphore sem = new Semaphore(2 * 16);
		int cont = 0;
		for (int i = 0; i < v; i++) {
			if (ls.contains(i))
				continue;
			try {
				sem.acquire();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			final int currVertex = i;
			cont++;
			if (cont % 10000 == 0)
				System.out.println("Current vertex generated: " + cont);
			exec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						createVertex(currVertex);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					sem.release();

				}
			});

		}

		for (final long currVertex : ls.toArray()) {
			exec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						createVertex(currVertex);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
		}

		exec.shutdown();
		try {
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		outwriter.close();

		BufferedWriter inwriter = new BufferedWriter(new FileWriter(new File(fName + ".in")));
		for (Entry<Long, TLongHashSet> l : in.entrySet()) {
			long[] edges = l.getValue().toArray();
			for (long to : edges) {
				inwriter.write(l.getKey() + sep + to + "\n");
			}
		}
		inwriter.close();
	}

	private TLongHashSet createEdges(int vertices) {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		TLongHashSet ret = new TLongHashSet();
		int max_edges = (int) (e * c * random.nextDouble());
		while (ret.size() < max_edges) {
			long vid = (long) (random.nextDouble() * vertices);
			if (!ret.contains(vid))
				ret.add(vid);
		}

		// for (int i = 0; i < e; i++) {
		// double r = random.nextDouble();
		// if (r <= c) {
		// if (ls.size() > 0) {
		// double src = random.nextDouble();
		// if (src >= 0.5)
		// ret.add(ls.get((int) (random.nextDouble() * ls.size())));
		// else
		// ret.add((long) (random.nextDouble() * vertices));
		// } else
		// ret.add((long) (random.nextDouble() * vertices));
		// }
		// }
		return ret;
	}

	public static Iterator<Pair<Long, long[]>> read(String file, String sep) throws NumberFormatException, IOException {
		return new GraphReader(file, sep);
	}
}
