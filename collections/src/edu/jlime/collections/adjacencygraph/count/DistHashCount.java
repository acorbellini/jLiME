package edu.jlime.collections.adjacencygraph.count;

import java.util.Iterator;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intint.DistIntIntHashtable;
import edu.jlime.jd.JobCluster;

public class DistHashCount extends CountResult {

	private static final long serialVersionUID = 1498670055476047552L;

	String table;

	public DistHashCount(String hash) {
		this.table = hash;
	}

	public Iterator<int[]> iterator(JobContext c) {
		try {
			return DistIntIntHashtable.get(table, c).iterator();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public void delete(JobCluster c) throws Exception {
		DistIntIntHashtable.delete(table, c);
	}
}