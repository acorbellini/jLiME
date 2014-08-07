package edu.jlime.jd;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;

public class JobExecutorBroadcastImpl implements JobExecutorBroadcast {

	RPCDispatcher disp;

	Peer local;

	List<Peer> dest = new ArrayList<Peer>();

	String cliID;

	String targetID;

	public JobExecutorBroadcastImpl(RPCDispatcher disp, List<Peer> dest,
			String cliID, String targetID) {
		this.disp = disp;
		this.dest.addAll(dest);
		this.cliID = cliID;
		this.targetID = targetID;
	}

	public void execute(JobContainer jobcontainer) throws Exception {
		disp.multiCallAsync(dest, cliID, targetID, "execute",
				new Object[] { jobcontainer });
	}

	public void result(Object object, UUID uuid, JobNode jobnode)
			throws Exception {
		disp.multiCallAsync(dest, cliID, targetID, "result", new Object[] {
				object, uuid, jobnode });
	}

}