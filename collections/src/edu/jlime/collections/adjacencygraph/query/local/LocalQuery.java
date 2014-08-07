package edu.jlime.collections.adjacencygraph.query.local;

import edu.jlime.collections.adjacencygraph.query.Query;
import edu.jlime.collections.adjacencygraph.query.QueryCache;

public abstract class LocalQuery<T> implements Query<T> {

	private boolean cache = true;

	public void setCache(boolean cache) {
		this.cache = cache;
	}

	@Override
	public final T query() throws Exception {
		if (!cache)
			return exec();
		T res = QueryCache.get(this);
		if (res == null) {
			synchronized (this) {
				res = QueryCache.get(this);
				if (res == null) {
					res = exec();
					QueryCache.put(this, res);
				}
			}
		}
		return res;
	}

	protected abstract T exec() throws Exception;

}
