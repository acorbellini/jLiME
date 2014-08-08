package edu.jlime.collections.adjacencygraph;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

import edu.jlime.collections.adjacencygraph.query.UserQuery;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.jd.JobCluster;

public class RemoteAdjacencyGraph implements Closeable, AdjacencyGraph,
		Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3153612628245292494L;

	private Mapper mapper;

	private String map;

	private StoreConfig config;

	private transient JobCluster cluster;

	@Override
	public UserQuery getUser(int id) {
		return get(new int[] { id });
	}

	@Override
	public UserQuery get(int[] ids) {
		return new UserQuery(this, ids);
	}

	public RemoteAdjacencyGraph(StoreConfig config, JobCluster cluster,
			Mapper mapper) throws Exception {
		this.map = config.getStoreName();
		this.mapper = mapper;
		this.config = config;
		this.cluster = cluster;
		this.cluster.broadcast(new AdyacencyGraphInitJob(config.getStoreName(),
				config));
	}

	@Override
	public void close() throws IOException {
		try {
			cluster.broadcast(new CloseJob(map, config));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Mapper getMapper() {
		return mapper;
	}

	public String getMapName() {
		return map;
	}

	public JobCluster getCluster() {
		return cluster;
	}

}
