package edu.jlime.collections.adjacencygraph.query.local;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import edu.jlime.collections.adjacencygraph.query.ForEachQueryProc;

public class LocalForEachQuery<T> extends LocalQuery<Map<Integer, T>> {

	private LocalListQuery q;

	private ForEachQueryProc<T> proc;

	public LocalForEachQuery(LocalListQuery q, ForEachQueryProc<T> proc) {
		setCache(false);
		this.q = q;
		this.proc = proc;
	}

	@Override
	public Map<Integer, T> exec() throws Exception {
		final ConcurrentHashMap<Integer, T> ret = new ConcurrentHashMap<Integer, T>();
		int[] users = q.query();
		ExecutorService exec = Executors.newFixedThreadPool(100);
		final Semaphore sem = new Semaphore(100);
		int cont = 0;
		for (final int i : users) {
			if (cont % 1000 == 0)
				System.out.println(cont + "Processing user " + i);
			cont++;
			sem.acquire();
			exec.execute(new Runnable() {

				@Override
				public void run() {

					try {
						ret.put(i,
								proc.call(new LocalUserQuery(q.getStore(), i)));
					} catch (Exception e) {
						e.printStackTrace();
					}
					sem.release();
				}

			});

		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		return ret;
	}

}
