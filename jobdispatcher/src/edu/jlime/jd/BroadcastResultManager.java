package edu.jlime.jd;

import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.core.cluster.Peer;
import edu.jlime.jd.job.ResultManager;

final class BroadcastResultManager<R> extends ResultManager<R> {

	private final BroadcastException exception = new BroadcastException(
			"Broadcast Exception");

	private final Map<ClientNode, R> resultMap = new Hashtable<>();

	private final Semaphore sem;

	Logger log = Logger.getLogger(BroadcastResultManager.class);

	BroadcastResultManager(int size) {
		super(size);
		this.sem = new Semaphore(-size + 1);
	}

	@Override
	public void handleException(Exception res, String jobID, ClientNode fromID) {
		exception.put(fromID.getPeer(), res);
		sem.release();
	}

	@Override
	public void handleResult(R res, String jobID, ClientNode from) {
		if (res != null) {
			resultMap.put(from, res);
		} else
			log.warn("Broadcast result is null.");

		if (log.isDebugEnabled())
			log.debug("Releasing semaphore " + sem.availablePermits());
		sem.release();
	}

	public void waitResults() {
		if (log.isDebugEnabled())
			log.debug("Broadcast result manager waiting for "
					+ sem.availablePermits() + " results.");
		try {
			sem.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void addException(Peer p, Exception e) {
		exception.put(p, e);
	}

	public BroadcastException getException() {
		return exception;
	}

	public Map<ClientNode, R> getRes() {
		return resultMap;
	}

	@Override
	public String toString() {
		return "Broadcast Result Manager: " + super.getRemainingResults();
	}
}