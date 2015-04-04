package edu.jlime.graphly.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.traversal.Dir;

public class TopUsersExtractor {

	public static void main(String[] args) throws Exception {

		final Graphly graphly = Graphly.build(8);
		
		final GraphlyGraph g = graphly.getGraph(args[0]);

		// Thread.sleep(2000);

		BufferedReader reader = new BufferedReader(new FileReader(new File(
				args[0])));

		final BufferedWriter writer = new BufferedWriter(new FileWriter(
				new File(args[1])));

		String line = "";
		final AtomicInteger count = new AtomicInteger(0);
		final long[] at = new long[] {};
		final Float maxIS = new Float(args[2]);
		final Integer min = new Integer(args[3]);
		final Semaphore sem = new Semaphore(15);
		ExecutorService exec = Executors.newFixedThreadPool(10);

		while ((line = reader.readLine()) != null) {
			final Long vid = new Long(line);

			sem.acquire();
			exec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						long init = System.currentTimeMillis();
						int followersCount = 0;
						try {
							followersCount = g.getEdgesCount(Dir.IN, vid, at);
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						long x = System.currentTimeMillis() - init;
						if (x > 10) {
							System.out.println("FOLLOWERS: " + x + ": "
									+ followersCount + " loc : "
									+ g.getHash().getNode(vid));
						}
						// init = System.currentTimeMillis();
						init = System.currentTimeMillis();
						int followeesCount = 0;
						try {
							followeesCount = g.getEdgesCount(Dir.OUT, vid, at);
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						// int followeesCount = followees == null ? 0 :
						// followees.length;
						// int followersCount = followers == null ? 0 :
						// followers.length;
						x = System.currentTimeMillis() - init;
						if (x > 10) {
							System.out.println("FOLLOWEES: " + x + ": "
									+ followeesCount + " loc : "
									+ g.getHash().getNode(vid));
						}
						float IS = (followersCount - followeesCount)
								/ (float) (followersCount + followeesCount);

						IS = (IS + 1) / 2;

						if (IS < maxIS && IS > 0.01 && followeesCount > min) {
							synchronized (writer) {
								try {
									writer.write(vid + " " + followeesCount
											+ " " + followersCount + " " + IS
											+ "\r\n");
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
							count.incrementAndGet();
						}
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
		reader.close();
		writer.close();
		System.out.println("Se encontraron " + count + " usuarios.");
	}
}
