package edu.jlime.collections.adjacencygraph.query;

import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.GetType;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

public interface ListQuery extends Query<int[]> {

	public abstract ListQuery followees();

	public abstract ListQuery followers();

	public abstract CountQuery count(GetType type);

	public abstract CountQuery countFollowers();

	public abstract CountQuery countFollowees();

	public abstract ListQuery remove(ListQuery toRem);

	public abstract ListQuery intersect(ListQuery listQuery);

	public abstract ListQuery union(ListQuery query);

	public abstract Query<Integer> size();

	public abstract <T> Query<Map<Integer, T>> foreach(ForEachQueryProc<T> proc);

	public abstract ListQuery getToRemove();

	public abstract ListQuery filterBy(ListQuery filter);

	public abstract ListQuery getFilter();

	public abstract ListQuery neighbours();

	public abstract <T> Query<Map<Integer, T>> map(MapProc<T> mq);

	public abstract Query<TIntObjectHashMap<int[]>> followeesMap();

	public abstract Query<TIntObjectHashMap<int[]>> followersMap();

	public abstract Query<TIntObjectHashMap<int[]>> neighboursMap();

	public abstract Query<TIntIntHashMap> neighboursSizesMap();

}