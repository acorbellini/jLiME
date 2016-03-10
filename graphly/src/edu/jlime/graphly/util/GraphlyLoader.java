package edu.jlime.graphly.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.util.Pair;

public class GraphlyLoader {

	public static void main(String[] args) throws Exception {
		String action = args[0];
		String servers = args[1];
		String graph = args[2];
		String sep = args[3];
		String fileIn = args[4];
		String fileOut = args[5];

		Integer serverInt = Integer.valueOf(servers);

		Graphly graphly = Graphly.build(serverInt);
		Graph g = graphly.getGraph(graph);

		// GraphlyServer server = GraphlyServerFactory
		// .loopback("D:/Graphly/konect").build();
		// server.start();
		// Graphly graphly = server.getGraphlyClient();

		// Graph g = graphly.getGraph("konect");

		if (action.equals("validate")) {
			new GraphlyLoader(g).validate(fileIn, sep, Dir.IN);
			new GraphlyLoader(g).validate(fileOut, sep, Dir.OUT);
		} else {
			new GraphlyLoader(g).load(fileIn, sep, Dir.IN);
			new GraphlyLoader(g).load(fileOut, sep, Dir.OUT);

		}
		graphly.close();
		// server.stop();
	}

	private Graph g;

	public GraphlyLoader(Graph graphly) {
		this.g = graphly;
	}

	public void validate(String file, String sep, final Dir dir)
			throws Exception, IOException {
		Iterator<Pair<Long, long[]>> adj = GraphlySintetic.read(file, sep);
		int cont = 0;
		int last = -1;
		while (adj.hasNext()) {
			Pair<java.lang.Long, long[]> pair = (Pair<java.lang.Long, long[]>) adj
					.next();

			long[] value = pair.getValue();
			if (value != null) {
				cont += value.length;
				if (((int) Math.floor(cont / 10000)) != last) {
					System.out.println("Validate state: " + cont + " edges.");
					last = (int) Math.floor(cont / 10000);
				}

				long[] dbList = g.getEdges(dir, pair.getKey());
				if (!Arrays.equals(dbList, pair.getValue()))
					throw new Exception(dir + " edges badly loaded.");
			}
		}
	}

	public void load(String fname, String sep, final Dir dir)
			throws Exception, IOException, InterruptedException {
		int cont = 0;
		final AtomicInteger contProm = new AtomicInteger(0);
		final AtomicLong sum = new AtomicLong(0);
		Iterator<Pair<Long, long[]>> adj = GraphlySintetic.read(fname, sep);

		ExecutorService exec = Executors.newFixedThreadPool(64);

		final Semaphore sem = new Semaphore(64);
		int last = -1;
		while (adj.hasNext()) {
			final Pair<java.lang.Long, long[]> pair = adj.next();
			final long[] value = pair.getValue();
			if (value != null) {
				cont += value.length;
				if (((int) Math.floor(cont / 10000)) != last) {
					System.out.println("Load state: " + cont + " edges.");
					last = (int) Math.floor(cont / 10000);
				}

				sem.acquire();
				exec.execute(new Runnable() {

					@Override
					public void run() {
						try {
							long init = System.nanoTime();
							g.addEdges(pair.getKey(), dir, value);
							sum.addAndGet(System.nanoTime() - init);
							contProm.incrementAndGet();
						} catch (Exception e) {
							e.printStackTrace();
						}
						sem.release();
					}
				});
			}

		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		System.out.println(contProm.get());

		System.out.println(sum.get() / contProm.get());

	}
}
