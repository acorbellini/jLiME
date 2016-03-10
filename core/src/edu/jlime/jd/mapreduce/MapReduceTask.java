package edu.jlime.jd.mapreduce;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.job.ResultManager;

@SuppressWarnings({ "unchecked", "serial", "rawtypes" })
public abstract class MapReduceTask<T, R, SR> implements Job<R> {

	// Para enganchar con el sistema de resultados de ejecuci�n de trabajos.

	public static class TaskResultManager<SR> extends ResultManager<SR> {

		MapReduceTask task;

		public TaskResultManager(MapReduceTask task) {
			this.task = task;
		}

		@Override
		public void handleException(Exception res, String jid, Node from) {
			task.error(from.getPeer(), res);
		}

		@Override
		public void handleResult(SR res, String jid, Node from) {
			Logger.getLogger(TaskResultManager.class).info("Received Results from " + from);
			task.result(res);
		}
	}

	private T data;

	private boolean dontCacheSubResults;

	ForkJoinException exceptions = null;

	private Semaphore lock;

	ArrayList<SR> subresults = new ArrayList<>();

	public MapReduceTask(T data) {
		this.data = data;
	}

	@Override
	public R call(JobContext env, Node peer) throws Exception {
		return exec(data, env);
	}

	public void error(Peer p, Exception res) {
		synchronized (this) {
			if (exceptions == null)
				exceptions = new ForkJoinException();
		}
		exceptions.put(p, res);
		lock.release();
	}

	public R exec(ClientCluster c) throws Throwable {
		if (c.getLocalNode().isExec())
			return c.getLocalNode().exec(this);
		else
			return c.getAnyExecutor().exec(this);
	}

	private R exec(T data, JobContext c) throws Exception {

		Logger log = Logger.getLogger(getClass());

		Map<Job<SR>, Node> m = map(data, c);

		lock = new Semaphore(-m.size() + 1);

		for (Entry<Job<SR>, Node> jobServer : m.entrySet()) {
			try {
				jobServer.getValue().execAsync(jobServer.getKey(), new TaskResultManager(this));
			} catch (Exception e) {
				log.error("Error executing map-reduce job ", e);
			}
		}

		try {
			lock.acquire();
		} catch (InterruptedException e) {
			log.error("", e);
		}

		if (exceptions != null && !exceptions.isEmpty()) {
			exceptions.addSubRes(subresults);
			throw exceptions;
		}

		if (!dontCacheSubResults)
			return red(subresults);
		else
			return red(new ArrayList<SR>());
	};

	public abstract Map<Job<SR>, Node> map(T data, JobContext cluster) throws Exception;

	public boolean processSubResult(SR subres) {
		return false;
	}

	public abstract R red(ArrayList<SR> subres) throws Exception;

	public void result(SR subres) {
		if (!dontCacheSubResults || !processSubResult(subres))
			synchronized (subresults) {
				if (subres != null && !dontCacheSubResults)
					subresults.add(subres);
			}
		lock.release();
	}

	public void setDontCacheSubResults(boolean dontCacheSubResults) {
		this.dontCacheSubResults = dontCacheSubResults;
	}

}
