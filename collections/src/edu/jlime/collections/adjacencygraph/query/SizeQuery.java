package edu.jlime.collections.adjacencygraph.query;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.Mapper;

public class SizeQuery extends RemoteQuery<Integer> {

	private static final long serialVersionUID = 2240598754834442040L;

	private RemoteListQuery query;

	public SizeQuery(RemoteListQuery listQuery) {
		super(listQuery.getGraph());
	}

	@Override
	public String getMapName() {
		return query.getMapName();
	}

	@Override
	public Mapper getMapper() {
		return query.getMapper();
	}

	@Override
	protected Integer doExec(JobContext c) throws Exception {
		return query.exec(c).length;
	}

}
