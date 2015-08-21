package edu.jlime.graphly.rec.salsa;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.rec.Repeat;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.set.hash.TLongHashSet;

public class SalsaRepeat implements Repeat<long[]> {
	private String authKey;
	private String hubKey;
	private long[] all;
	private Object defaultauth;
	private Object defaulthub;

	public SalsaRepeat(String authKey, String hubKey, TLongHashSet authSet,
			TLongHashSet hubSet) {
		this.authKey = authKey;
		this.hubKey = hubKey;
		this.defaultauth = 1f / authSet.size();
		this.defaulthub = 1f / hubSet.size();
		TLongHashSet sub = new TLongHashSet(authSet);
		sub.addAll(hubSet);
		all = sub.toArray();
		Arrays.sort(all);
	}

	@Override
	public Object exec(long[] before, GraphlyGraph g) throws Exception {
		final SubGraph sg = g.getSubGraph("salsa-sub", all);

		ExecutorService exec = Executors.newFixedThreadPool(Runtime
				.getRuntime().availableProcessors(), new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setName("Salsa Repeat Step");
				t.setDaemon(true);
				return t;
			}
		});

		final Semaphore max = new Semaphore(Runtime.getRuntime()
				.availableProcessors());

		sg.loadProperties(authKey, defaultauth);
		sg.loadProperties(hubKey, defaulthub);

		final Semaphore sem = new Semaphore(-before.length + 1);
		final Map<Long, Map<String, Object>> temps = new ConcurrentHashMap<Long, Map<String, Object>>();
		for (final long vid : before) {
			max.acquire();

			exec.execute(new Runnable() {

				@Override
				public void run() {
					Map<String, Object> ret;
					try {
						ret = salsa(sg, vid);
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

		exec.shutdown();

		g.setTempProperties(before, temps);

		return before;
	}

	private Map<String, Object> salsa(final SubGraph sg, long vid)
			throws Exception {
		Map<String, Object> ret = new HashMap<>();
		float authCalc = 0f;
		long[] inEdges = sg.getEdges(Dir.IN, vid);
		for (final long v : inEdges) {
			Float res = (Float) sg.getTemp("in-" + v, new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					Float res = 0f;
					long[] outV = sg.getEdges(Dir.OUT, v);
					for (long w : outV) {
						int inW = sg.getEdgesCount(Dir.IN, w);
						if (inW > 0)
							res += ((Float) sg.getProperty(w, authKey,
									defaultauth)) / (outV.length * inW);
					}
					return res;
				}
			});
			authCalc += res;
		}

		float hubCalc = 0f;
		long[] outEdges = sg.getEdges(Dir.OUT, vid);
		for (final long v : outEdges) {
			Float res = (Float) sg.getTemp("out-" + v, new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					Float res = 0f;
					long[] inV = sg.getEdges(Dir.IN, v);
					for (long w : inV) {
						int outW = sg.getEdgesCount(Dir.OUT, w);
						if (outW > 0)
							res += ((Float) sg.getProperty(w, hubKey,
									defaulthub)) / (inV.length * outW);
					}
					return res;
				}
			});
			hubCalc += res;
		}

		ret.put(authKey, authCalc);
		ret.put(hubKey, hubCalc);
		return ret;
	}
}
