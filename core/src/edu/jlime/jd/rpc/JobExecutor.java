package edu.jlime.jd.rpc;

import java.util.UUID;

import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobContainer;

public interface JobExecutor {

	public abstract void execute(JobContainer j) throws Exception;

	public abstract void result(Object res, UUID jobID, ClientNode req)
			throws Exception;

}