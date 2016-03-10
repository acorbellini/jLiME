package edu.jlime.jd.task;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.jlime.jd.Node;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.job.ResultManager;
import edu.jlime.jd.mapreduce.ForkJoinException;

public abstract class TaskBase<T> implements Task<T> {
	private Logger log = Logger.getLogger(TaskBase.class);

	@Override
	public <R> R execute(ResultListener<T, R> listener) throws Exception {
		return this.execute(Integer.MAX_VALUE, listener);
	}

	@Override
	public <R> R execute(int maxjobs, final ResultListener<T, R> listener)
			throws Exception {
		return execute(maxjobs, listener, Integer.MAX_VALUE);
	}

	public <R> R execute(int maxjobs, final ResultListener<T, R> listener,
			int timeout) throws Exception {
		Map<Job<T>, Node> map = getMap();

		final int permits = map.keySet().size();
		final Semaphore sem = new Semaphore(-permits + 1);

		final Semaphore max = new Semaphore(maxjobs);

		Iterator<Entry<Job<T>, Node>> it = map.entrySet().iterator();

		final ForkJoinException excep = new ForkJoinException();

		while (it.hasNext()) {
			Entry<Job<T>, Node> entry = it.next();
			it.remove();
			try {
				max.acquire();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}

			if (!excep.isEmpty())
				throw excep;

			try {
				if (log.isDebugEnabled())
					log.debug("Sending to execute " + entry.getKey().toString()
							+ "  to  " + entry.getValue().toString());
				entry.getValue().execAsync(entry.getKey(),
						new ResultManager<T>() {

							@Override
							public void handleException(Exception res,
									String job, Node peer) {
								try {
									listener.onFailure(res);
								} catch (Exception e) {
									e.printStackTrace();
								}
								try {
									sem.release(permits);
								} catch (Exception e) {
									e.printStackTrace();
								}
								try {
									max.release();
								} catch (Exception e) {
									e.printStackTrace();
								}

								excep.put(peer.getPeer(), res);
							}

							@Override
							public void handleResult(T res, String job,
									Node peer) {
								try {
									listener.onSuccess(res);
								} catch (Throwable e) {
									try {
										sem.release(permits);
									} catch (Exception e2) {
										e.printStackTrace();
									}
									try {
										max.release();
									} catch (Exception e2) {
										e.printStackTrace();
									}

									excep.put(peer.getPeer(),
											new Exception(
													"Exception during success processing",
													e));

									return;
								}
								try {
									sem.release();
								} catch (Exception e) {
									e.printStackTrace();
								}
								try {
									max.release();
								} catch (Exception e) {
									e.printStackTrace();
								}

							}
						});
			} catch (Exception e) {
				listener.onFailure(e);
				sem.release();
			}
		}
		try {
			boolean res = sem.tryAcquire(timeout, TimeUnit.MILLISECONDS);
			if (!res)
				throw new Exception("Job Timed Out");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (!excep.isEmpty())
			throw excep;

		return listener.onFinished();
	}

	protected abstract Map<Job<T>, Node> getMap();

}