package edu.jlime.graphly;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.util.GraphlySintetic;

public class GraphlyLoader {

	public static void main(String[] args) throws Exception {
		if (args[2].equals("validate"))
			new GraphlyLoader().validate(Integer.valueOf(args[0]), args[1]);
		else
			new GraphlyLoader().load(Integer.valueOf(args[0]), args[1]);
	}

	private void validate(Integer min, String string) throws Exception {
		Graphly g = Graphly.build(min);
		Iterator<Pair<Long, long[]>> adj = GraphlySintetic.read(string);
		while (adj.hasNext()) {
			Pair<java.lang.Long, long[]> pair = (Pair<java.lang.Long, long[]>) adj
					.next();
			if (pair.getKey() < 0) {
				long[] dbList = g.getEdges(Dir.IN, -(pair.getKey() + 1));
				if (!Arrays.equals(dbList, pair.getValue()))
					throw new Exception("DB OUT edges badly loaded.");
			} else {
				long[] dbList = g.getEdges(Dir.OUT, pair.getKey());
				if (!Arrays.equals(dbList, pair.getValue()))
					throw new Exception("DB OUT edges badly loaded.");
			}

		}
		g.close();
	}

	private void load(Integer min, String fname) throws Exception {
		Graphly g = Graphly.build(min);

		Iterator<Pair<Long, long[]>> adj = GraphlySintetic.read(fname);

		ExecutorService exec = Executors.newFixedThreadPool(2);

		Semaphore sem = new Semaphore(10);

		while (adj.hasNext()) {
			final Pair<java.lang.Long, long[]> pair = adj.next();
			sem.acquire();
			exec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						if (pair.getKey() < 0)
							g.addEdges(-(pair.getKey() + 1), Dir.IN,
									pair.getValue());
						else
							g.addEdges(pair.getKey(), Dir.OUT, pair.getValue());
					} catch (Exception e) {
						e.printStackTrace();
					}
					sem.release();
				}
			});

		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		g.close();
	}
}
