package edu.jlime.jd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.esotericsoftware.minlog.Log;

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

public class ClientCluster implements Iterable<Node> {

	private Dispatcher disp;

	private Peer client;

	private Node localPeer;

	private Map<String, Node> byName = new ConcurrentHashMap<String, Node>();

	private Map<Peer, Node> clis = new ConcurrentHashMap<Peer, Node>();

	public ClientCluster(Dispatcher jobDispatcher, Peer clientID) {
		this.disp = jobDispatcher;
		this.client = clientID;
		this.localPeer = new Node(jobDispatcher.getLocalPeer(), clientID, disp);
	}

	public ArrayList<Node> getExecutors() {
		ArrayList<Peer> execs = new ArrayList<Peer>(disp.getExecutors());
		ArrayList<Node> execCli = new ArrayList<>(execs.size());
		for (Peer jobNode : execs) {
			execCli.add(getClientFor(jobNode));
		}
		return execCli;
	}

	public Node getByName(String jobNode) {
		Node ret = byName.get(jobNode);
		if (ret == null) {
			synchronized (this) {
				ret = byName.get(jobNode);
				while (ret == null) {
					getPeers();
					ret = byName.get(jobNode);
					if (ret == null)
						try {
							Log.warn("Waiting for peer " + jobNode + " current state: " + byName);
							Thread.sleep(5000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
				}
			}
		}
		return ret;
	}

	public Node getClientFor(Peer jobNode) {
		Node ret = clis.get(jobNode);
		if (ret == null)
			synchronized (jobNode) {
				ret = clis.get(jobNode);
				if (ret == null) {
					ret = new Node(jobNode, client, disp);
					clis.put(jobNode, ret);
					byName.put(jobNode.getName(), ret);
				}
			}
		return ret;
	}

	public Metrics getMetrics() {
		return disp.getMetrics();
	}

	public <R> Map<Node, R> mcast(List<Node> p, Job<R> j) throws Exception {
		ClientJob<R> cliJob = new ClientJob<R>(j, client);
		return disp.mcast(p, cliJob);
	}

	public <R> void mcastAsync(Collection<Node> p, Job<R> j) throws Exception {
		ClientJob<R> cliJob = new ClientJob<R>(j, client);
		disp.mcastAsync(p, cliJob);
	}

	public RemoteInputStream getInputStream(UUID streamID, Node from) {
		return disp.getInputStream(streamID, from);
	}

	public RemoteOutputStream getOutputStream(UUID streamID, Node streamSource) {
		return disp.getOutputStream(streamID, streamSource);
	}

	public Node getLocalNode() {
		return localPeer;
	}

	public ArrayList<Node> getPeers() {
		ArrayList<Peer> peers = disp.getPeers();
		ArrayList<Node> copy = new ArrayList<>();
		for (Peer jobNode : peers) {
			copy.add(getClientFor(jobNode));
		}
		return copy;
	}

	public int executorsSize() {
		return disp.executorsSize();
	}

	public <R> Map<Node, R> broadcast(Job<R> j) throws Exception {
		return mcast(getExecutors(), j);
	}

	public <R> void broadcastAsync(Job<R> j) throws Exception {
		mcastAsync(getExecutors(), j);
	}

	public <R> MCastStreamResult broadcastStream(StreamJob j) throws Exception {
		return mcastStream(getExecutors(), j);
	}

	public Node getAnyExecutor() throws Exception {
		ArrayList<Node> copy = new ArrayList<>(getExecutors());
		if (copy.isEmpty())
			throw new Exception("Empty Server List");
		return copy.get((int) (Math.random() * copy.size()));

	}

	public MCastStreamResult mcastStream(ArrayList<Node> peers, StreamJob j) {
		List<StreamResult> results = new ArrayList<>();
		for (Node peer : peers)
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

	public <R> Map<Node, R> chain(Job<R> j) throws Exception {
		return chain(getExecutors(), j, false);
	}

	public <R> void chainAsync(Job<R> j) throws Exception {
		chain(getExecutors(), j, true);
	}

	public <R> Map<Node, R> chain(Collection<Node> list, Job<R> j, boolean async) throws Exception {
		ArrayList<Node> remaining = new ArrayList<>();
		for (Node jobNode : list) {
			remaining.add(Node.copy(jobNode, disp));
		}
		Node current = remaining.remove(0);
		ChainJob<R> chain = new ChainJob<R>(j, remaining);
		if (async) {
			current.execAsync(chain);
			return null;
		} else
			return current.exec(chain);
	}

	public <R> Map<Node, R> chain(Collection<Node> peers, Job<R> j) throws Exception {
		return chain(peers, j, false);
	}

	public <R> void chainAsync(Collection<Node> peers, Job<R> j) throws Exception {
		chain(peers, j, true);
	}

	public CompositeMetrics<Node> getInfo() throws Exception {
		return new CompositeMetrics<Node>(broadcast(new MetricsQuery()));
	}

	@Override
	public Iterator<Node> iterator() {
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
