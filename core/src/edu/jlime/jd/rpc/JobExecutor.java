package edu.jlime.jd.rpc;

import java.util.UUID;

import edu.jlime.jd.JobContainer;
import edu.jlime.jd.Node;

public interface JobExecutor {

	public abstract void execute(JobContainer j) throws Exception;

	public abstract void result(Object res, UUID jobID, Node req) throws Exception;

}