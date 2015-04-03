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

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.traversal.Dir;

public class GraphlyLoader {

	private static final String OUT = "out";
	private static final String IN = "in";

	public static void main(String[] args) throws Exception {
		String action = args[0];
		String servers = args[1];
		String sep = args[2];
		String fileIn = args[3];
		String fileOut = args[4];
		if (action.equals("validate")) {
			new GraphlyLoader().validate(Integer.valueOf(servers), fileIn, sep,
					IN);
			new GraphlyLoader().validate(Integer.valueOf(servers), fileOut,
					sep, OUT);
		} else {
			new GraphlyLoader().load(Integer.valueOf(servers), fileIn, sep, IN);
			new GraphlyLoader().load(Integer.valueOf(servers), fileOut, sep,
					OUT);

		}
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
				if (dir.equals(IN))
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
		final AtomicInteger contProm = new AtomicInteger(0);
		final AtomicLong sum = new AtomicLong(0);
		Iterator<Pair<Long, long[]>> adj = GraphlySintetic.read(fname, sep);

		ExecutorService exec = Executors.newFixedThreadPool(4);

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
							if (dir.equals(IN))
								edgeDir = Dir.IN;
							long init = System.nanoTime();
							g.addEdges(pair.getKey(), edgeDir, value);
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

		g.close();
	}
}
