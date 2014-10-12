package edu.jlime.jd;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.jd.rpc.JobExecutor;

public class JobExecutorServerImpl implements JobExecutor {

	RPCDispatcher disp;

	Peer local;

	Peer dest;

	Peer cliID;

	String targetID;

	public JobExecutorServerImpl(RPCDispatcher disp, Peer dest, Peer cliID,
			String targetID) {
		this.disp = disp;
		this.dest = dest;
		this.cliID = cliID;
		this.targetID = targetID;
	}

	public void execute(JobContainer jobcontainer) throws Exception {
		disp.callAsync(dest, cliID, targetID, "execute",
				new Object[] { jobcontainer });
	}

	public void result(Object object, UUID uuid, ClientNode jobnode)
			throws Exception {
		disp.callAsync(dest, cliID, targetID, "result", new Object[] { object,
				uuid, jobnode });
	}

}