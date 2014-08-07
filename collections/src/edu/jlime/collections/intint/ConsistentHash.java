package edu.jlime.collections.intint;

import java.io.Serializable;

import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;

//Version preliminar de consistent hashing.
//Falta agregar Merkle Trees para chequeo de r�plicas.

public class ConsistentHash implements Serializable {

	private static final long serialVersionUID = -823320688891325902L;

	private int ring_size = 100;

	private int clusterSize;

	private JobNode[] serversPerToken;

	public ConsistentHash(JobCluster iCluster) throws Exception {
		this.clusterSize = iCluster.executorsSize();
		if (clusterSize == 0)
			throw new Exception("I can't performn consistent hashing using ");
		serversPerToken = new JobNode[ring_size];
		int i = 0;
		while (i != serversPerToken.length) {
			for (JobNode p : iCluster) {
				if (i < serversPerToken.length)
					serversPerToken[i++] = p;
			}
		}
	}

	public JobNode getServerForKey(int k) {
		long hash = k * 5700357409661598721L;
		int pos = Math.abs((int) (hash % ring_size));
		return serversPerToken[pos];
	}
}
