package edu.jlime.collections.adjacencygraph.query;

public interface CountQuery {

	public abstract TopQuery top(int top);

	public abstract TopQuery top(int top, boolean delete);

	public abstract CountQuery remove(RemoteListQuery followees)
			throws Exception;

	public abstract ListQuery getToremove();

}