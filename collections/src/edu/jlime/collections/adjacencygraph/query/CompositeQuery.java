package edu.jlime.collections.adjacencygraph.query;

public abstract class CompositeQuery<T, R> extends RemoteQuery<R> {

	private static final long serialVersionUID = -2587945249534968709L;

	private RemoteQuery<T> query;

	public CompositeQuery(RemoteQuery<T> query) {
		super(query.getGraph());
		this.query = query;
	}

	public RemoteQuery<T> getQuery() {
		return query;
	}
}
