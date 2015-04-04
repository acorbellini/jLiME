package edu.jlime.jd.job;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;

public abstract class ResultManager<R> {

	Logger log = Logger.getLogger(ResultManager.class);

	private AtomicInteger remainingResults;

	public int getRemainingResults() {
		return remainingResults.get();
	}

	public ResultManager() {
		this(1);
	}

	public ResultManager(int res) {
		this.remainingResults = new AtomicInteger(res);
	}

	protected abstract void handleException(Exception res, String jobID,
			ClientNode fromID);

	protected abstract void handleResult(R res, String jobID, ClientNode fromID);

	@Override
	public String toString() {
		return "Result Manager : " + getRemainingResults() + " - class :  "
				+ getClass();
	}

	public void manageResult(JobDispatcher jd, UUID jobID, R res, ClientNode req) {
		int current_remaining = remainingResults.decrementAndGet();

		if (current_remaining < 0) {
			if (res != null && Exception.class.isAssignableFrom(res.getClass()))
				log.error("Unexpected exception from job " + jobID
						+ " from server " + req, (Throwable) res);
			else if (log.isDebugEnabled())
				log.debug("Received result, but remaining result count is 0 for "
						+ jobID + " from server " + req);
			return;
		} else if (current_remaining == 0) {
			if (log.isDebugEnabled())
				log.debug("Removing completed job: " + jobID);
			jd.removeMap(jobID);
		}

		if (res != null && Exception.class.isAssignableFrom(res.getClass())) {
			if (log.isDebugEnabled())
				log.debug("Handling exception for " + jobID + " from server "
						+ req);
			handleException((Exception) res, jobID.toString(),
					ClientNode.copy(req, jd));
		} else {
			if (log.isDebugEnabled())
				log.debug("Handling result for " + jobID + " from server "
						+ req);
			handleResult(res, jobID.toString(), ClientNode.copy(req, jd));
		}
	}
}
