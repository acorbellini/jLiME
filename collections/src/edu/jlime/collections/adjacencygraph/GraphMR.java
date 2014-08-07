package edu.jlime.collections.adjacencygraph;

import edu.jlime.jd.mapreduce.MapReduceTask;

public abstract class GraphMR<R, SR> extends MapReduceTask<int[], R, SR> {

	private static final long serialVersionUID = -2702984476334205064L;

	String mapName;

	Mapper mapper;

	public GraphMR(int[] data, String mapName, Mapper mapper) {
		super(data);
		this.mapName = mapName;
		this.mapper = mapper;
	}

	public Mapper getMapper() {
		return mapper;
	}

	public void setMapper(Mapper mapper) {
		this.mapper = mapper;
	}

	public String getMapName() {
		return mapName;
	}
}