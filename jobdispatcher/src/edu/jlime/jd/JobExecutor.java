package edu.jlime.jd;

import java.util.UUID;

public interface JobExecutor {

	public abstract void execute(JobContainer j) throws Exception;

	public abstract void result(Object res, UUID jobID, JobNode req)
			throws Exception;

}