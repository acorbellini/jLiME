package edu.jlime.collections.adjacencygraph.query.local;

import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.GetType;
import edu.jlime.collections.adjacencygraph.query.CountQuery;
import edu.jlime.collections.adjacencygraph.query.ForEachQueryProc;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.adjacencygraph.query.MapProc;
import edu.jlime.collections.adjacencygraph.query.MultiGetQuery;
import edu.jlime.collections.adjacencygraph.query.Query;
import edu.jlime.collections.intintarray.db.Store;
import gnu.trove.map.hash.TIntIntHashMap;

public abstract class LocalListQuery extends LocalQuery<int[]> implements
		ListQuery {

	private Store store;

	public LocalListQuery(Store store) {
		this.store = store;
	}

	public Store getStore() {
		return store;
	}

	@Override
	public ListQuery followees() {
		return get(GetType.FOLLOWEES);
	}

	@Override
	public ListQuery followers() {
		return get(GetType.FOLLOWERS);
	}

	public ListQuery get(GetType type) {
		return new LocalGetQuery(this, type);
	}

	@Override
	public CountQuery count(GetType type) {
		return new LocalCountQuery(this, type);
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
	public ListQuery intersect(ListQuery query) {
		return new LocalIntersectQuery(this, query);
	}

	@Override
	public ListQuery union(ListQuery query) {
		return new LocalUnionQuery(this, query);
	}

	@Override
	public Query<Integer> size() {
		return new LocalSizeQuery(this);
	}

	@Override
	public <T> Query<Map<Integer, T>> foreach(ForEachQueryProc<T> proc) {
		return new LocalForEachQuery(this, proc);
	}

	@Override
	public ListQuery neighbours() {
		return get(GetType.NEIGHBOURS);
	}

	@Override
	public <T> Query<Map<Integer, T>> map(MapProc<T> mq) {
		return new LocalMapQuery<T>(this, mq);
	}

	@Override
	public MultiGetQuery followeesMap() {
		return getMap(this, GetType.FOLLOWEES);
	}

	private MultiGetQuery getMap(LocalListQuery localListQuery,
			GetType followees) {
		return null;
	}

	@Override
	public MultiGetQuery followersMap() {
		return getMap(this, GetType.FOLLOWERS);
	}

	@Override
	public MultiGetQuery neighboursMap() {
		return getMap(this, GetType.NEIGHBOURS);
	}

	@Override
	public Query<TIntIntHashMap> neighboursSizesMap() {
		return new LocalMultiGetSizeQuery(this, GetType.NEIGHBOURS);
	}

	@Override
	public ListQuery remove(ListQuery toRem) {
		return this;
	}

	@Override
	public ListQuery getToRemove() {
		return null;
	}

	@Override
	public ListQuery getFilter() {
		return null;
	}

	@Override
	public ListQuery filterBy(ListQuery filter) {
		return this;
	}
}
