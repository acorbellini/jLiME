package edu.jlime.graphly.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.traversal.Dir;

public class GraphlyLoader {

	public static void main(String[] args) throws Exception {
		if (args[0].equals("validate"))
			new GraphlyLoader().validate(Integer.valueOf(args[1]), args[2],
					args[3], args[4]);
		else
			new GraphlyLoader().load(Integer.valueOf(args[1]), args[2],
					args[3], args[4]);
	}

	private void validate(Integer servers, String file, String sep, String dir)
			throws Exception, IOException {
		Graphly g = Graphly.build(servers);
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

				Dir edgeDir = Dir.OUT;
				if (dir.equals("in"))
					edgeDir = Dir.IN;
				long[] dbList = g.getEdges(edgeDir, pair.getKey());
				if (!Arrays.equals(dbList, pair.getValue()))
					throw new Exception(edgeDir + " edges badly loaded.");
			}
		}
		g.close();
	}

	private void load(Integer min, String fname, String sep, final String dir)
			throws Exception, IOException, InterruptedException {
		final Graphly g = Graphly.build(min);

		int cont = 0;

		Iterator<Pair<Long, long[]>> adj = GraphlySintetic.read(fname, sep);

		ExecutorService exec = Executors.newFixedThreadPool(8);

		final Semaphore sem = new Semaphore(10);
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
							Dir edgeDir = Dir.OUT;
							if (dir.equals("in"))
								edgeDir = Dir.IN;
							g.addEdges(pair.getKey(), edgeDir, value);
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
		g.close();
	}
}
