package edu.jlime.jd.task;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.job.ResultManager;

public abstract class TaskBase<T> implements Task<T> {

	@Override
	public <R> R execute(final ResultListener<T, R> listener) {

		Map<Job<T>, ClientNode> map = getMap();

		final Semaphore sem = new Semaphore(-map.keySet().size() + 1);

		for (Entry<Job<T>, ClientNode> entry : map.entrySet()) {
			try {
				entry.getValue().execAsync(entry.getKey(),
						new ResultManager<T>() {

							@Override
							public void handleException(Exception res,
									String job, ClientNode peer) {
								listener.onFailure(res);
								sem.release();

							}

							@Override
							public void handleResult(T res, String job,
									ClientNode peer) {
								listener.onSuccess(res);
								sem.release();
							}
						});
			} catch (Exception e) {
				listener.onFailure(e);
				sem.release();
			}
		}
		try {
			sem.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return listener.onFinished();
	}

	protected abstract Map<Job<T>, ClientNode> getMap();

}