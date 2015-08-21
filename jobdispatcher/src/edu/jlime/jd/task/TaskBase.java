package edu.jlime.jd.task;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.job.ResultManager;

public abstract class TaskBase<T> implements Task<T> {
	private Logger log = Logger.getLogger(TaskBase.class);

	@Override
	public <R> R execute(ResultListener<T, R> listener) {
		return this.execute(Integer.MAX_VALUE / 2, listener);
	}

	@Override
	public <R> R execute(int maxjobs, final ResultListener<T, R> listener) {

		Map<Job<T>, ClientNode> map = getMap();

		final Semaphore sem = new Semaphore(-map.keySet().size() + 1);

		final Semaphore max = new Semaphore(maxjobs);
		Iterator<Entry<Job<T>, ClientNode>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Entry<Job<T>, ClientNode> entry = it.next();
			it.remove();
			try {
				max.acquire();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			try {
				if (log.isDebugEnabled())
					log.debug("Sending to execute " + entry.getKey().toString()
							+ "  to  " + entry.getValue().toString());
				entry.getValue().execAsync(entry.getKey(),
						new ResultManager<T>() {

							@Override
							public void handleException(Exception res,
									String job, ClientNode peer) {
								try {
									listener.onFailure(res);
								} catch (Exception e) {
									e.printStackTrace();
								}
								try {
									sem.release();
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								try {
									max.release();
								} catch (Exception e) {
									e.printStackTrace();
								}
							}

							@Override
							public void handleResult(T res, String job,
									ClientNode peer) {
								try {
									listener.onSuccess(res);
								} catch (Exception e) {
									e.printStackTrace();
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
			sem.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return listener.onFinished();
	}

	protected abstract Map<Job<T>, ClientNode> getMap();

}