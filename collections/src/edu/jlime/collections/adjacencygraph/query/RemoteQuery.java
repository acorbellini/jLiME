package edu.jlime.collections.adjacencygraph.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;

import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.collections.adjacencygraph.RemoteAdjacencyGraph;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;

public abstract class RemoteQuery<R> implements Serializable, Query<R> {

	private static final long serialVersionUID = -1546784961626821249L;

	private long queryTime = 0;

	private boolean cacheQuery = true;

	private transient ClientCluster cluster;

	private RemoteAdjacencyGraph graph;

	public RemoteQuery(RemoteAdjacencyGraph graph) {
		this.graph = graph;
		this.cluster = graph.getCluster();
	}

	public Mapper getMapper() {
		return graph.getMapper();
	}

	public String getMapName() {
		return graph.getMapName();
	}

	public ClientCluster getCluster() {
		return cluster;
	}

	public final R exec(JobContext c) throws Exception {
		this.cluster = c.getCluster();
		long init = Calendar.getInstance().getTimeInMillis();
		R res = null;
		if (cacheQuery)
			res = QueryCache.get(this);
		if (res == null) {
			res = doExec(c);
			if (cacheQuery)
				QueryCache.put(this, res);
		}

		long end = Calendar.getInstance().getTimeInMillis();
		queryTime = end - init;
		return res;
	}

	@Override
	public final R query() throws Exception {
		R res = null;
		long init = Calendar.getInstance().getTimeInMillis();
		if (!cluster.getLocalNode().isExec()) {
			res = cluster.getExecutors().get(1).exec(
					new QueryJobWrapper<R>(this));
		} else
			res = cluster.getLocalNode().exec(new QueryJobWrapper<R>(this));
		long end = Calendar.getInstance().getTimeInMillis();
		queryTime = end - init;
		return res;
	}

	protected abstract R doExec(JobContext c) throws Exception;

	public Long getQueryTime() {
		return queryTime;
	}

	public Query<R> setCacheQuery(boolean b) {
		this.cacheQuery = b;
		return this;
	}

	public RemoteAdjacencyGraph getGraph() {
		return graph;
	}
}