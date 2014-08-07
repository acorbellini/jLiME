package edu.jlime.jd.job;

import java.util.UUID;

import org.apache.log4j.Logger;

import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.JobNode;

public abstract class ResultManager<R> {

	Logger log = Logger.getLogger(ResultManager.class);

	private int remainingResults = 1;

	public int getRemainingResults() {
		return remainingResults;
	}

	public ResultManager() {
	}

	public ResultManager(int res) {
		this.remainingResults = res;
	}

	protected abstract void handleException(Exception res, String jobID,
			JobNode fromID);

	protected abstract void handleResult(R res, String jobID, JobNode fromID);

	@Override
	public String toString() {
		return "Result Manager : " + getRemainingResults() + " - class :  "
				+ getClass();
	}

	public void manageResult(JobDispatcher jd, UUID jobID, R res, JobNode req) {
		int current_remaining;
		synchronized (this) {
			current_remaining = remainingResults--;
		}
		if (current_remaining < 0) {
			if (res != null && Exception.class.isAssignableFrom(res.getClass()))
				log.error("Unexpected exception from job " + jobID
						+ " from server " + req, (Throwable) res);
			else if (log.isDebugEnabled())
				log.debug("Remaining result count is 0 for " + jobID
						+ " from server " + req);
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
					JobNode.copy(req, jd));
		} else {
			if (log.isDebugEnabled())
				log.debug("Handling result for " + jobID + " from server "
						+ req);
			handleResult(res, jobID.toString(), JobNode.copy(req, jd));
		}
	}
}
