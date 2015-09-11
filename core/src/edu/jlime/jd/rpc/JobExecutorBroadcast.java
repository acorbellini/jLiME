package edu.jlime.jd.rpc;

import java.util.UUID;

import edu.jlime.jd.JobContainer;
import edu.jlime.jd.Node;

public interface JobExecutorBroadcast {

	public void execute(final JobContainer arg0) throws Exception;

	public void result(final Object arg0, final UUID arg1, final Node arg2) throws Exception;

}