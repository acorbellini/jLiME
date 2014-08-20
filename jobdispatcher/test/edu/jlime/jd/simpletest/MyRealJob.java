package edu.jlime.jd.simpletest;

import edu.jlime.client.JobContext;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.Job;

public class MyRealJob implements Job<Integer> {

	private static final long serialVersionUID = -493143877549962477L;

	private Integer data;

	public MyRealJob(Integer i) {
		this.data = i;
	}

	@Override
	public Integer call(JobContext env, ClientNode peer) {
		System.out.println(data);
		return data;
	}
}
