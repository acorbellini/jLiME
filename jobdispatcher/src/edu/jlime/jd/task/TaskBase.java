package edu.jlime.jd.task;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.job.ResultManager;

public abstract class TaskBase<R> implements Task<R> {

	@Override
	public <J extends Job<R>> void execute(final ResultListener<R> listener) {

		Map<Job<R>, JobNode> map = getMap();

		final Semaphore sem = new Semaphore(-map.keySet().size() + 1);

		for (Entry<Job<R>, JobNode> entry : map.entrySet()) {
			try {
				entry.getValue().execAsync(entry.getKey(),
						new ResultManager<R>() {

							@Override
							public void handleException(Exception res,
									String job, JobNode peer) {
								listener.onFailure(res);
								sem.release();

							}

							@Override
							public void handleResult(R res, String job,
									JobNode peer) {
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
		listener.onFinished();
	}

	protected abstract Map<Job<R>, JobNode> getMap();

}