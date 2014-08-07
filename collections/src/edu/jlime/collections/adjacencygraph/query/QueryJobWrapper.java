package edu.jlime.collections.adjacencygraph.query;

import edu.jlime.client.JobContext;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;

public class QueryJobWrapper<T> implements Job<T> {

	private static final long serialVersionUID = -8130560957888317420L;

	private RemoteQuery<T> query;

	public QueryJobWrapper(RemoteQuery<T> query) {
		this.query = query;
	}

	@Override
	public T call(JobContext ctx, JobNode peer) throws Exception {
		return query.exec(ctx);
	}

}
