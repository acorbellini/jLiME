package edu.jlime.graphly.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.server.GraphlyServer;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.set.hash.TLongHashSet;

public class TopUsersExtractor {

	public static void main(String[] args) throws Exception {

		// final GraphlyClient graphly = GraphlyClient.build(8);

		GraphlyServer s = GraphlyServerFactory.loopback(args[0]).build();
		s.start();

		Graphly graphly = s.getGraphlyClient();

		final Graph g = graphly.getGraph(args[1]);

		// Thread.sleep(2000);

		final BufferedWriter writer = new BufferedWriter(new FileWriter(new File(args[2])));

		final AtomicInteger count = new AtomicInteger(0);
		final TLongHashSet at = new TLongHashSet();
		final Semaphore sem = new Semaphore(15);
		ExecutorService exec = Executors.newFixedThreadPool(10);

		for (final Long vid : g.vertices()) {
			sem.acquire();
			exec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						int followersCount = 0;
						try {
							followersCount = g.getEdgesCount(Dir.IN, vid, at);
						} catch (Exception e1) {
							e1.printStackTrace();
						}
						int followeesCount = 0;
						try {
							followeesCount = g.getEdgesCount(Dir.OUT, vid, at);
						} catch (Exception e1) {
							e1.printStackTrace();
						}
						float IS = (followersCount - followeesCount) / (float) (followersCount + followeesCount);

						IS = (IS + 1) / 2;

						synchronized (writer) {
							try {
								writer.write(vid + "," + followeesCount + "," + followersCount + "," + IS + "\r\n");
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						count.incrementAndGet();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					sem.release();
				}
			});
		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		writer.close();
		s.stop();
		System.out.println("Se encontraron " + count + " usuarios.");
	}
}
