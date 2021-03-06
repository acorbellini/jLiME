package edu.jlime.jd;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.UUID;

import org.apache.log4j.Logger;

public class JobContainer implements Runnable, Serializable {

	private static final long serialVersionUID = 2576435314444491681L;

	private UUID jobID;

	private boolean noresponse = false;

	Node origin;

	private ClientJob<?> rJ;

	transient Dispatcher srv;

	public JobContainer(ClientJob<?> j, Node requestor) {
		this(j, UUID.randomUUID(), requestor);

	}

	public JobContainer(ClientJob<?> j, UUID id, Node requestor) {
		this.origin = requestor;
		this.rJ = j;
		this.jobID = id;
	}

	public ClientJob<?> getJob() {
		return rJ;
	};

	public UUID getJobID() {
		return jobID;
	}

	public Node getRequestor() {
		return origin;
	}

	public boolean isNoresponse() {
		return noresponse;
	}

	public void run() {
		Logger log = Logger.getLogger(JobContainer.class);
		if (log.isDebugEnabled())
			log.debug("Running jobwrapper with id " + jobID + " from " + origin);
		Object res;
		try {
			try {
				if (log.isDebugEnabled())
					log.debug("Calling job " + jobID + " from " + origin);
				res = rJ.call(srv.getEnv().getClientEnv(rJ.getClient()), origin);
				if (log.isDebugEnabled())
					log.debug("Finished call to job " + jobID + " from " + origin);
			} catch (Throwable e) {
				log.error("", e);
				res = new Exception("Exception calling job " + jobID + " from " + origin, e);
			}
			try {
				if (!isNoresponse() || Exception.class.isAssignableFrom(res.getClass())) {
					if (log.isDebugEnabled())
						log.debug("Sending result for job " + getJobID() + " to " + origin);
					srv.sendResult(res, origin, jobID, rJ.getClient());
				} else {
					if (log.isDebugEnabled())
						log.debug("Job " + jobID + " WILL NOT RESPOND");
				}
			} catch (NotSerializableException nse) {
				srv.sendResult(nse, origin, jobID, rJ.getClient());
				log.error("", nse);
			} catch (Exception e) {
				log.error("", e);
			}
		} catch (Exception e1) {
			log.error("", e1);
		}

	}

	public void setNoResponse(boolean b) {
		this.noresponse = b;
	}

	public void setrJ(ClientJob<?> j) {
		this.rJ = j;
	}

	public void setSrv(Dispatcher srv) {
		this.srv = srv;
	}

	public void setID(UUID id) {
		this.jobID = id;

	}

	@Override
	public String toString() {
		return "JobContainer [jobID=" + jobID + ", noresponse=" + noresponse + ", origin=" + origin + ", rJ=" + rJ
				+ "]";
	}

}
