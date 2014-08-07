package edu.jlime.jd;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.StreamResult;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.job.ResultManager;
import edu.jlime.jd.job.StreamJob;
import edu.jlime.metrics.metric.IMetrics;

public class JobNode implements Serializable {

	private static final long serialVersionUID = 1L;

	private String clientID;

	private Peer peer;

	transient private JobDispatcher jd;

	public JobNode(Peer p, String clientID, JobDispatcher disp) {
		this.peer = p;
		this.clientID = clientID;
		this.jd = disp;
	}

	public Peer getPeer() {
		return peer;
	}

	public <R> R exec(Job<R> j) throws Exception {
		ClientJob<R> cliJob = new ClientJob<>(j, clientID);
		return jd.execSync(this, cliJob);
	}

	public <R> void execAsync(Job<R> j, ResultManager<R> rm) throws Exception {
		ClientJob<R> cliJob = new ClientJob<>(j, clientID);
		jd.execAsync(this, cliJob, rm);
	}

	public <R> Future<R> execAsyncWithFuture(Job<R> j) {
		ClientJob<R> cliJob = new ClientJob<>(j, clientID);
		return jd.execAsyncWithFuture(this, cliJob);
	}

	public IMetrics getInfo() throws Exception {
		return jd.getMetrics();
	}

	public RemoteInputStream getInputStream(UUID streamIDOutput) {
		return jd.getInputStream(streamIDOutput, this);
	}

	public RemoteOutputStream getOutputStream(UUID streamIDInput) {
		return jd.getOutputStream(streamIDInput, this);
	}

	public Set<String> getTags() {
		HashSet<String> ret = new HashSet<>();
		String[] tags = peer.getData(JobDispatcher.TAGS).split(",");
		for (String t : tags) {
			ret.add(t);
		}
		return ret;
	}

	public boolean isExec() {
		boolean isExec = Boolean.valueOf(peer.getData(JobDispatcher.ISEXEC));
		return isExec;
	}

	public <R> void execAsync(Job<R> j) throws Exception {
		execAsync(j, null);
	}

	public StreamResult stream(final StreamJob stream) {
		try {
			execAsync(stream, new ResultManager<Boolean>() {

				@Override
				public void handleException(Exception res, String jobID,
						JobNode fromID) {
					res.printStackTrace();
					stream.setFinished(true);
				}

				@Override
				public void handleResult(Boolean res, String jobID,
						JobNode fromID) {
					stream.setFinished(true);
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new StreamResult(getOutputStream(stream.getStreamIDInput()),
				getInputStream(stream.getStreamIDOutput()));
	}

	public String getID() {
		return peer.getID();
	}

	public static JobNode copy(JobNode req, JobDispatcher disp) {
		return new JobNode(req.getPeer(), req.clientID, disp);
	}

	public String getName() {
		return peer.getName();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof JobNode))
			return false;
		JobNode other = (JobNode) obj;
		return peer.equals(other.getPeer());
	}

	@Override
	public int hashCode() {
		return peer.hashCode();
	}

	@Override
	public String toString() {
		return peer.toString();
	}
}
