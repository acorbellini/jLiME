package edu.jlime.jd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.core.cluster.BroadcastOutputStream;
import edu.jlime.core.cluster.MCastStreamResult;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.StreamResult;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.job.StreamJob;
import edu.jlime.metrics.metric.CompositeMetrics;
import edu.jlime.metrics.metric.Metrics;

//This is a proxy to the JobCluster, one per client exists in servers.

public class JobCluster implements Iterable<JobNode> {

	private JobDispatcher disp;

	private String clientID;

	private JobNode localPeer;

	public JobCluster(JobDispatcher jobDispatcher, String clientID) {
		this.disp = jobDispatcher;
		this.clientID = clientID;
		this.localPeer = new JobNode(jobDispatcher.getLocalPeer(), clientID,
				disp);
	}

	public ArrayList<JobNode> getExecutors() {
		ArrayList<Peer> execs = new ArrayList<Peer>(disp.getExecutors());
		ArrayList<JobNode> execCli = new ArrayList<>();
		for (Peer jobNode : execs) {
			execCli.add(new JobNode(jobNode, clientID, disp));
		}
		return execCli;
	}

	public Metrics getMetrics() {
		return disp.getMetrics();
	}

	public <R> Map<JobNode, R> mcast(List<JobNode> p, Job<R> j)
			throws BroadcastException {
		ClientJob<R> cliJob = new ClientJob<R>(j, clientID);
		return disp.mcast(p, cliJob);
	}

	public <R> void mcastAsync(Collection<JobNode> p, Job<R> j)
			throws Exception {
		ClientJob<R> cliJob = new ClientJob<R>(j, clientID);
		disp.mcastAsync(p, cliJob);
	}

	public RemoteInputStream getInputStream(UUID streamID, JobNode from) {
		return disp.getInputStream(streamID, from);
	}

	public RemoteOutputStream getOutputStream(UUID streamID,
			JobNode streamSource) {
		return disp.getOutputStream(streamID, streamSource);
	}

	public JobNode getLocalNode() {
		return localPeer;
	}

	public ArrayList<JobNode> getPeers() {
		ArrayList<Peer> peers = disp.getPeers();
		ArrayList<JobNode> copy = new ArrayList<>();
		for (Peer jobNode : peers) {
			copy.add(new JobNode(jobNode, clientID, disp));
		}
		return copy;
	}

	public int executorsSize() {
		return disp.executorsSize();
	}

	private Random rand = new Random();

	public <R> Map<JobNode, R> broadcast(Job<R> j) throws Exception {
		return mcast(getExecutors(), j);
	}

	public <R> void broadcastAsync(Job<R> j) throws Exception {
		mcastAsync(getExecutors(), j);
	}

	public <R> MCastStreamResult broadcastStream(StreamJob j) throws Exception {
		return mcastStream(getExecutors(), j);
	}

	public JobNode getAnyExecutor() throws Exception {
		ArrayList<JobNode> copy = new ArrayList<>(getExecutors());
		if (copy.isEmpty())
			throw new Exception("Empty Server List");
		return copy.get(rand.nextInt(copy.size()));

	}

	public MCastStreamResult mcastStream(ArrayList<JobNode> peers, StreamJob j) {
		List<StreamResult> results = new ArrayList<>();
		for (JobNode peer : peers)
			results.add(peer.stream(j));

		List<RemoteOutputStream> streams = new ArrayList<>();

		for (StreamResult streamResult : results)
			streams.add(streamResult.getOs());

		BroadcastOutputStream os = new BroadcastOutputStream(streams);
		MCastStreamResult res = new MCastStreamResult(os);
		for (StreamResult streamResult : results)
			res.addInput(streamResult.getIs());

		return res;
	}

	public <R> Map<JobNode, R> chain(Job<R> j) throws Exception {
		return chain(getExecutors(), j, false);
	}

	public <R> void chainAsync(Job<R> j) throws Exception {
		chain(getExecutors(), j, true);
	}

	public <R> Map<JobNode, R> chain(Collection<JobNode> list, Job<R> j,
			boolean async) throws Exception {
		ArrayList<JobNode> remaining = new ArrayList<>();
		for (JobNode jobNode : list) {
			remaining.add(JobNode.copy(jobNode, disp));
		}
		JobNode current = remaining.remove(0);
		ChainJob<R> chain = new ChainJob<R>(j, remaining);
		if (async) {
			current.execAsync(chain);
			return null;
		} else
			return current.exec(chain);
	}

	public <R> Map<JobNode, R> chain(Collection<JobNode> peers, Job<R> j)
			throws Exception {
		return chain(peers, j, false);
	}

	public <R> void chainAsync(Collection<JobNode> peers, Job<R> j)
			throws Exception {
		chain(peers, j, true);
	}

	public CompositeMetrics<JobNode> getInfo() throws Exception {
		return new CompositeMetrics<JobNode>(broadcast(new MetricsQuery()));
	}

	@Override
	public Iterator<JobNode> iterator() {
		return getPeers().iterator();
	}
}
