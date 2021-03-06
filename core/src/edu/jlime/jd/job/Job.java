package edu.jlime.jd.job;

import java.io.Serializable;

import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;

public interface Job<R> extends Serializable {
	//
	// private boolean doNotReplicateIfLocal = false;
	//
	// private String clientID;
	//
	// public Job(String cliID) {
	// this.clientID = cliID;
	// }
	//
	// public void setDoNotReplicateIfLocal(boolean doNotReplicateIfLocal) {
	// this.doNotReplicateIfLocal = doNotReplicateIfLocal;
	// }
	//
	// public boolean doNotReplicateIfLocal() {
	// return doNotReplicateIfLocal;
	// }
	//
	// public String getClient() {
	// return clientID;
	// }

	public abstract R call(JobContext env, Node peer) throws Exception;
}
