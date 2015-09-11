package edu.jlime.jd;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.StreamResult;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.core.transport.Address;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.job.ResultManager;
import edu.jlime.jd.job.StreamJob;
import edu.jlime.metrics.metric.IMetrics;

public class Node implements Serializable {

	private static final long serialVersionUID = 1L;

	private Peer clientID;

	private Peer peer;

	transient private Dispatcher jd;

	public Node(Peer p, Peer clientID, Dispatcher disp) {
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

	// public <R> Future<R> execAsyncWithFuture(Job<R> j) {
	// ClientJob<R> cliJob = new ClientJob<>(j, clientID);
	// return jd.execAsyncWithFuture(this, cliJob);
	// }

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
		String[] tags = peer.getData(Dispatcher.TAGS).split(",");
		for (String t : tags) {
			ret.add(t);
		}
		return ret;
	}

	public boolean isExec() {
		boolean isExec = Boolean.valueOf(peer.getData(Dispatcher.ISEXEC));
		return isExec;
	}

	public <R> void execAsync(Job<R> j) throws Exception {
		execAsync(j, null);
	}

	public StreamResult stream(final StreamJob stream) {
		try {
			execAsync(stream, new ResultManager<Boolean>() {

				@Override
				public void handleException(Exception res, String jobID, Node fromID) {
					res.printStackTrace();
					stream.setFinished(true);
				}

				@Override
				public void handleResult(Boolean res, String jobID, Node fromID) {
					stream.setFinished(true);
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new StreamResult(getOutputStream(stream.getStreamIDInput()), getInputStream(stream.getStreamIDOutput()));
	}

	public Address getID() {
		return peer.getAddress();
	}

	public static Node copy(Node req, Dispatcher disp) {
		return new Node(req.getPeer(), req.clientID, disp);
	}

	public String getName() {
		return peer.getName();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Node))
			return false;
		Node other = (Node) obj;
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

	public Peer getClient() {
		return clientID;
	}
}
