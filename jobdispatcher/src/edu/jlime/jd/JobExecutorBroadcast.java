package edu.jlime.jd;

import java.util.UUID;

public interface JobExecutorBroadcast {

	public void execute(JobContainer jobcontainer) throws Exception;

	public void result(Object object, UUID uuid, JobNode jobnode)
			throws Exception;

}