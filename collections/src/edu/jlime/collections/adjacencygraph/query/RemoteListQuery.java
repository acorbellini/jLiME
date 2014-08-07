package edu.jlime.collections.adjacencygraph.query;

import java.util.Map;

import edu.jlime.collections.adjacencygraph.RemoteAdjacencyGraph;
import edu.jlime.collections.adjacencygraph.get.GetType;
import gnu.trove.map.hash.TIntIntHashMap;

public abstract class RemoteListQuery extends RemoteQuery<int[]> implements
		ListQuery {

	private static final long serialVersionUID = -501354250073255846L;

	private ListQuery toremove = null;

	private ListQuery filter = null;

	public RemoteListQuery(RemoteAdjacencyGraph graph) {
		super(graph);
	}

	// public ListQuery(AdjacencyGraph graph) {
	// super(graph);
	// }

	@Override
	public RemoteListQuery followees() {
		return get(GetType.FOLLOWEES);
	}

	@Override
	public RemoteListQuery followers() {
		return get(GetType.FOLLOWERS);
	}

	public RemoteListQuery get(GetType type) {
		return new GetQuery(this, type);
	}

	@Override
	public CountQuery count(GetType type) {
		return new RemoteCountQuery(this, type);
	}

	@Override
	public CountQuery countFollowers() {
		return count(GetType.FOLLOWERS);
	}

	@Override
	public CountQuery countFollowees() {
		return count(GetType.FOLLOWEES);
	}

	@Override
	public ListQuery remove(ListQuery toRem) {
		toremove = toRem;
		return this;
	}

	@Override
	public RemoteListQuery intersect(ListQuery listQuery) {
		return new IntersectQuery(this, listQuery);
	}

	@Override
	public ListQuery union(ListQuery query) {
		return new UnionQuery(this, query);
	}

	@Override
	public Query<Integer> size() {
		return new SizeQuery(this);
	}

	@Override
	public <T> RemoteForEachQuery<T> foreach(ForEachQueryProc<T> proc) {
		return new RemoteForEachQuery<T>(this, proc);
	}

	@Override
	public ListQuery getToRemove() {
		return toremove;
	}

	@Override
	public ListQuery filterBy(ListQuery filter) {
		this.filter = filter;
		return this;
	}

	@Override
	public ListQuery getFilter() {
		return filter;
	}

	@Override
	public RemoteListQuery neighbours() {
		return get(GetType.NEIGHBOURS);
	}

	@Override
	public <T> Query<Map<Integer, T>> map(MapProc<T> mq) {
		return new RemoteMapQuery<T>(mq, this);
	}

	@Override
	public MultiGetQuery followeesMap() {
		return new MultiGetQuery(this, GetType.FOLLOWEES);
	}

	@Override
	public MultiGetQuery followersMap() {
		return new MultiGetQuery(this, GetType.FOLLOWERS);
	}

	@Override
	public MultiGetQuery neighboursMap() {
		return new MultiGetQuery(this, GetType.NEIGHBOURS);
	}

	@Override
	public Query<TIntIntHashMap> neighboursSizesMap() {
		return new MultiGetSizeQuery(this, GetType.NEIGHBOURS);
	}

}