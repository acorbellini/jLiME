package edu.jlime.jd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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

public class ClientCluster implements Iterable<ClientNode> {

	private JobDispatcher disp;

	private Peer client;

	private ClientNode localPeer;

	private Map<String, ClientNode> byName = new ConcurrentHashMap<String, ClientNode>();

	private Map<Peer, ClientNode> clis = new ConcurrentHashMap<Peer, ClientNode>();

	public ClientCluster(JobDispatcher jobDispatcher, Peer clientID) {
		this.disp = jobDispatcher;
		this.client = clientID;
		this.localPeer = new ClientNode(jobDispatcher.getLocalPeer(), clientID,
				disp);
	}

	public ArrayList<ClientNode> getExecutors() {
		ArrayList<Peer> execs = new ArrayList<Peer>(disp.getExecutors());
		ArrayList<ClientNode> execCli = new ArrayList<>(execs.size());
		for (Peer jobNode : execs) {
			execCli.add(getClientFor(jobNode));
		}
		return execCli;
	}

	public ClientNode getByName(String jobNode) {
		ClientNode ret = byName.get(jobNode);
		if (ret == null) {
			synchronized (this) {
				ret = byName.get(jobNode);
				if (ret == null) {
					getPeers();
					ret = byName.get(jobNode);
				}
			}
		}
		return ret;
	}

	public ClientNode getClientFor(Peer jobNode) {
		ClientNode ret = clis.get(jobNode);
		if (ret == null)
			synchronized (jobNode) {
				ret = clis.get(jobNode);
				if (ret == null) {
					ret = new ClientNode(jobNode, client, disp);
					clis.put(jobNode, ret);
					byName.put(jobNode.getName(), ret);
				}
			}
		return ret;
	}

	public Metrics getMetrics() {
		return disp.getMetrics();
	}

	public <R> Map<ClientNode, R> mcast(List<ClientNode> p, Job<R> j)
			throws Exception {
		ClientJob<R> cliJob = new ClientJob<R>(j, client);
		return disp.mcast(p, cliJob);
	}

	public <R> void mcastAsync(Collection<ClientNode> p, Job<R> j)
			throws Exception {
		ClientJob<R> cliJob = new ClientJob<R>(j, client);
		disp.mcastAsync(p, cliJob);
	}

	public RemoteInputStream getInputStream(UUID streamID, ClientNode from) {
		return disp.getInputStream(streamID, from);
	}

	public RemoteOutputStream getOutputStream(UUID streamID,
			ClientNode streamSource) {
		return disp.getOutputStream(streamID, streamSource);
	}

	public ClientNode getLocalNode() {
		return localPeer;
	}

	public ArrayList<ClientNode> getPeers() {
		ArrayList<Peer> peers = disp.getPeers();
		ArrayList<ClientNode> copy = new ArrayList<>();
		for (Peer jobNode : peers) {
			copy.add(getClientFor(jobNode));
		}
		return copy;
	}

	public int executorsSize() {
		return disp.executorsSize();
	}

	public <R> Map<ClientNode, R> broadcast(Job<R> j) throws Exception {
		return mcast(getExecutors(), j);
	}

	public <R> void broadcastAsync(Job<R> j) throws Exception {
		mcastAsync(getExecutors(), j);
	}

	public <R> MCastStreamResult broadcastStream(StreamJob j) throws Exception {
		return mcastStream(getExecutors(), j);
	}

	public ClientNode getAnyExecutor() throws Exception {
		ArrayList<ClientNode> copy = new ArrayList<>(getExecutors());
		if (copy.isEmpty())
			throw new Exception("Empty Server List");
		return copy.get((int) (Math.random() * copy.size()));

	}

	public MCastStreamResult mcastStream(ArrayList<ClientNode> peers,
			StreamJob j) {
		List<StreamResult> results = new ArrayList<>();
		for (ClientNode peer : peers)
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

	public <R> Map<ClientNode, R> chain(Job<R> j) throws Exception {
		return chain(getExecutors(), j, false);
	}

	public <R> void chainAsync(Job<R> j) throws Exception {
		chain(getExecutors(), j, true);
	}

	public <R> Map<ClientNode, R> chain(Collection<ClientNode> list, Job<R> j,
			boolean async) throws Exception {
		ArrayList<ClientNode> remaining = new ArrayList<>();
		for (ClientNode jobNode : list) {
			remaining.add(ClientNode.copy(jobNode, disp));
		}
		ClientNode current = remaining.remove(0);
		ChainJob<R> chain = new ChainJob<R>(j, remaining);
		if (async) {
			current.execAsync(chain);
			return null;
		} else
			return current.exec(chain);
	}

	public <R> Map<ClientNode, R> chain(Collection<ClientNode> peers, Job<R> j)
			throws Exception {
		return chain(peers, j, false);
	}

	public <R> void chainAsync(Collection<ClientNode> peers, Job<R> j)
			throws Exception {
		chain(peers, j, true);
	}

	public CompositeMetrics<ClientNode> getInfo() throws Exception {
		return new CompositeMetrics<ClientNode>(broadcast(new MetricsQuery()));
	}

	@Override
	public Iterator<ClientNode> iterator() {
		return getPeers().iterator();
	}

	@Override
	public String toString() {
		return disp.getPeers().toString();
	}

	public void clear() {
		byName.clear();
		clis.clear();
	}
}