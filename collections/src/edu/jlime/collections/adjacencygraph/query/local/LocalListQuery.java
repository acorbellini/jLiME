package edu.jlime.collections.adjacencygraph.query.local;

import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.Dir;
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
		return get(Dir.OUT);
	}

	@Override
	public ListQuery followers() {
		return get(Dir.IN);
	}

	public ListQuery get(Dir type) {
		return new LocalGetQuery(this, type);
	}

	@Override
	public CountQuery count(Dir type) {
		return new LocalCountQuery(this, type);
	}

	@Override
	public CountQuery countFollowers() {
		return count(Dir.IN);
	}

	@Override
	public CountQuery countFollowees() {
		return count(Dir.OUT);
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
		return get(Dir.BOTH);
	}

	@Override
	public <T> Query<Map<Integer, T>> map(MapProc<T> mq) {
		return new LocalMapQuery<T>(this, mq);
	}

	@Override
	public MultiGetQuery followeesMap() {
		return getMap(this, Dir.OUT);
	}

	private MultiGetQuery getMap(LocalListQuery localListQuery,
			Dir followees) {
		return null;
	}

	@Override
	public MultiGetQuery followersMap() {
		return getMap(this, Dir.IN);
	}

	@Override
	public MultiGetQuery neighboursMap() {
		return getMap(this, Dir.BOTH);
	}

	@Override
	public Query<TIntIntHashMap> neighboursSizesMap() {
		return new LocalMultiGetSizeQuery(this, Dir.BOTH);
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
