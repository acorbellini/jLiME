package edu.jlime.collections.adjacencygraph.query;

public interface Query<R> {

	public abstract R query() throws Exception;
}