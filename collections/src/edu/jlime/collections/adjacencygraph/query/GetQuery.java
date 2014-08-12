package edu.jlime.collections.adjacencygraph.query;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.collections.adjacencygraph.get.GetMR;
import edu.jlime.collections.adjacencygraph.get.GetType;
import gnu.trove.set.hash.TIntHashSet;

public class GetQuery extends RemoteListQuery {

	private static final long serialVersionUID = 6928522839313053238L;

	private RemoteQuery<int[]> query;

	private GetType type;

	public Query<int[]> getQuery() {
		return query;
	}

	public GetQuery(RemoteQuery<int[]> query, GetType type) {
		super(query.getGraph());
		this.query = query;
		this.type = type;
	}

	@Override
	public int[] doExec(JobContext c) throws Exception {
		Logger log = Logger.getLogger(GetQuery.class);

		// if (log.isDebugEnabled())
		log.info("Executing MR query with mapper on map " + getMapName());

		int[] subres = new GetMR(query.exec(c), getMapName(), getMapper(), type)
				.exec(c.getCluster());
		TIntHashSet set = new TIntHashSet(subres);
		log.info("Removing toRemove users.");
		if (getToRemove() != null)
			set.removeAll(getToRemove().query());
		log.info("Filtering users.");
		if (getFilter() != null)
			set.retainAll(getFilter().query());
		int[] array = set.toArray();
		// Arrays.sort(array);
		return array;
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
	public boolean equals(Object obj) {
		if (!(obj instanceof GetQuery))
			return false;
		GetQuery other = (GetQuery) obj;
		return this.getQuery().equals(other.getQuery())
				&& ((getToRemove() == null && other.getToRemove() == null) || (getToRemove()
						.equals(other.getToRemove())))
				&& ((getFilter() == null && other.getFilter() == null) || (getFilter()
						.equals(other.getFilter()))) && type.equals(other.type);
	}

	@Override
	public int hashCode() {
		return getQuery().hashCode();
	}
}