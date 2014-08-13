package edu.jlime.jd.mapreduce;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;
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
		public void handleException(Exception res, String jid, JobNode from) {
			task.error(res);
		}

		@Override
		public void handleResult(SR res, String jid, JobNode from) {
			task.result(res);
		}
	}

	private T data;

	private boolean dontCacheSubResults;

	MapReduceException exceptions = new MapReduceException();

	private Semaphore lock;

	ArrayList<SR> subresults = new ArrayList<>();

	public MapReduceTask(T data) {
		this.data = data;
	}

	@Override
	public R call(JobContext env, JobNode peer) throws Exception {
		return exec(data, env);
	}

	public void error(Exception res) {
		exceptions.add(res);
		lock.release();
	}

	public R exec(JobCluster c) throws Exception {
		if (c.getLocalNode().isExec())
			return c.getLocalNode().exec(this);
		else
			return c.getAnyExecutor().exec(this);
	}

	private R exec(T data, JobContext c) throws Exception {

		Logger log = Logger.getLogger(getClass());

		Map<Job<SR>, JobNode> m = map(data, c);

		lock = new Semaphore(-m.size() + 1);

		for (Entry<Job<SR>, JobNode> jobServer : m.entrySet()) {
			try {
				jobServer.getValue().execAsync(jobServer.getKey(),
						new TaskResultManager(this));
			} catch (Exception e) {
				log.error("Error executing map-reduce job ", e);
			}
		}

		try {
			lock.acquire();
		} catch (InterruptedException e) {
			log.error("", e);
		}

		if (!exceptions.isEmpty()) {
			exceptions.addSubRes(subresults);
			throw exceptions;
		}

		if (!dontCacheSubResults)
			return red(subresults);
		else
			return red(new ArrayList<SR>());
	};

	public abstract Map<Job<SR>, JobNode> map(T data, JobContext cluster)
			throws Exception;

	public void processSubResult(SR subres) {

	}

	public abstract R red(ArrayList<SR> subres);

	public void result(SR subres) {
		synchronized (subresults) {
			if (subres != null && !dontCacheSubResults)
				subresults.add(subres);
		}
		processSubResult(subres);
		lock.release();
	}

	public void setDontCacheSubResults(boolean dontCacheSubResults) {
		this.dontCacheSubResults = dontCacheSubResults;
	}

}
