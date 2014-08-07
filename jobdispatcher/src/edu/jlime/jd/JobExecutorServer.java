package edu.jlime.jd;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;

public class JobExecutorServer implements JobExecutor {

	RPCDispatcher disp;

	List<Peer> dest = new ArrayList<Peer>();

	String cliID;

	String targetID;

	public JobExecutorServer(RPCDispatcher disp, Peer local, Peer dest,
			String cliID, String targetID) {
		this.disp = disp;
		this.dest.add(dest);
		this.cliID = cliID;
		this.targetID = targetID;
	}

	public JobExecutorServer(RPCDispatcher disp, Peer local, List<Peer> dest,
			String cliID, String targetID) {
		this.disp = disp;
		this.dest.addAll(dest);
		this.cliID = cliID;
		this.targetID = targetID;
	}

	public void execute(JobContainer jobcontainer) throws Exception {
		if (dest.size() > 0)
			disp.multiCallAsync(dest, cliID, targetID, "execute",
					new Object[] { jobcontainer });
		else
			disp.callAsync(dest.get(0), cliID, targetID, "execute",
					new Object[] { jobcontainer });
	}

	public void result(Object object, UUID uuid, JobNode jobnode)
			throws Exception {
		if (dest.size() > 0)
			disp.multiCallAsync(dest, cliID, targetID, "result", new Object[] {
					object, uuid, jobnode });
		else
			disp.callAsync(dest.get(0), cliID, targetID, "result",
					new Object[] { object, uuid, jobnode });
	}

}