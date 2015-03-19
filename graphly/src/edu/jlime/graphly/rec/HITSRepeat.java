package edu.jlime.graphly.rec;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.traversal.Dir;

class HITSRepeat implements Repeat<long[]> {
	private static final int MAX_THREADS = 64;
	private String authKey;
	private String hubKey;
	private long[] current;
	static transient volatile ExecutorService exec;

	public HITSRepeat(String authKey, String hubKey, long[] current) {
		this.authKey = authKey;
		this.hubKey = hubKey;
		this.current = current;
	}

	@Override
	public Object exec(long[] before, Graphly g) throws Exception {
		if (exec == null) {
			synchronized (this) {
				if (exec == null)
					exec = Executors.newFixedThreadPool(MAX_THREADS,
							new ThreadFactory() {

								@Override
								public Thread newThread(Runnable r) {
									Thread t = Executors.defaultThreadFactory()
											.newThread(r);
									t.setName("Salsa Repeat Step");
									return t;
								}
							});
			}
		}
		final Semaphore max = new Semaphore(MAX_THREADS * 2);

		Logger log = Logger.getLogger(HITSRepeat.class);

		final SubGraph sg = g.getSubGraph("hits-sub", current);

		final Map<Long, Map<String, Object>> temps = new ConcurrentHashMap<>();
		final Semaphore sem = new Semaphore(-before.length + 1);

		log.info("Executing HITS function on " + before.length);
		for (final long vid : before) {
			max.acquire();
			exec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						Map<String, Object> ret = hitsforvid(sg, vid);
						temps.put(vid, ret);
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
		g.setTempProperties(before, temps);

		return before;
	}

	private Map<String, Object> hitsforvid(SubGraph sg, long vid)
			throws Exception {
		Map<String, Object> ret = new HashMap<>();
		float sumAuth = 0f;
		float sumAuthQuad = 0f;
		int contAuth = 0;
		for (long in : sg.getEdges(Dir.IN, vid)) {
			final long curr = in;

			Float currHub = (Float) sg.getProperty(curr, hubKey,
					(float) Math.sqrt(1f / current.length));
			float quad = currHub * currHub;
			sumAuth += currHub;
			sumAuthQuad += quad;
			contAuth++;
		}

		float sumHub = 0f;
		float sumHubQuad = 0f;
		int contHub = 0;
		for (long out : sg.getEdges(Dir.OUT, vid)) {
			final long curr = out;

			Float currAuth = (Float) sg.getProperty(curr, authKey,
					(float) Math.sqrt(1f / current.length));
			float quad = currAuth * currAuth;
			sumHub += currAuth;
			sumHubQuad += quad;
			contHub++;
		}
		ret.put(authKey,
				contAuth == 0 ? 0 : sumAuth / (float) Math.sqrt(sumAuthQuad));
		ret.put(hubKey,
				contHub == 0 ? 0 : sumHub / (float) Math.sqrt(sumHubQuad));
		return ret;
	}
}