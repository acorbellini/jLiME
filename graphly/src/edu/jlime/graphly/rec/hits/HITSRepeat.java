package edu.jlime.graphly.rec.hits;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.rec.Repeat;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.map.hash.TLongFloatHashMap;

public class HITSRepeat implements Repeat<long[]> {
	private String authKey;
	private String hubKey;
	private long[] current;

	public HITSRepeat(String authKey, String hubKey, long[] current) {
		this.authKey = authKey;
		this.hubKey = hubKey;
		this.current = current;
	}

	@Override
	public Object exec(long[] before, final Graph g) throws Exception {
		ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
				new ThreadFactory() {

					@Override
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory().newThread(r);
						t.setName("Salsa Repeat Step");
						return t;
					}
				});
		final Semaphore max = new Semaphore(Runtime.getRuntime().availableProcessors());

		Logger log = Logger.getLogger(HITSRepeat.class);

		final SubGraph sg = g.getSubGraph("hits-sub", current);
		final TLongFloatHashMap auth = new TLongFloatHashMap();
		final TLongFloatHashMap hub = new TLongFloatHashMap();
		final Semaphore sem = new Semaphore(-before.length + 1);

		log.info("Executing HITS function on " + before.length);
		for (final long vid : before) {
			max.acquire();
			exec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						float sumAuth = 0f;
						long[] incomingEdges = sg.getEdges(Dir.IN, vid);
						for (long in : incomingEdges)
							sumAuth += g.getFloat(in, hubKey, 1f / current.length);

						float sumHub = 0f;
						long[] outgoingEdges = sg.getEdges(Dir.OUT, vid);
						for (long out : outgoingEdges)
							sumHub += g.getFloat(out, authKey, 1f / current.length);

						synchronized (auth) {
							auth.put(vid, sumAuth);
							hub.put(vid, sumHub);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					sem.release();
					max.release();
				}
			});
		}
		sem.acquire();
		log.info("Setting temp properties for " + before.length);

		g.setTempFloats(hubKey, false, hub);
		g.setTempFloats(authKey, false, auth);

		exec.shutdown();
		return before;
	}
}