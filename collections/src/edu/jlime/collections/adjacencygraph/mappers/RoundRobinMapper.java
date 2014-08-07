package edu.jlime.collections.adjacencygraph.mappers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.core.cluster.Peer;
import edu.jlime.jd.JobNode;
import gnu.trove.list.array.TIntArrayList;

//Simple Round Robin

public class RoundRobinMapper extends Mapper {

	private static final long serialVersionUID = -2914997038447380314L;

	@Override
	public Map<JobNode, TIntArrayList> map(int[] data, JobContext env) {
		Logger log = Logger.getLogger(RoundRobinMapper.class);
		HashMap<JobNode, TIntArrayList> div = new HashMap<JobNode, TIntArrayList>();

		ArrayList<JobNode> serverList = env.getCluster().getExecutors();
		if (log.isDebugEnabled())
			log.debug("Mapping " + data.length + " between "
					+ serverList.size());
		int count = 0;
		for (int i : data) {
			JobNode p = serverList.get(count);
			count = (count + 1) % serverList.size();
			TIntArrayList uList = div.get(p);
			if (uList == null) {
				uList = new TIntArrayList();
				div.put(p, uList);
			}
			uList.add(i);
		}
		if (log.isDebugEnabled())
			log.debug("Resulting list (size " + div.size() + ")");
		return div;
	}

	public static void main(String[] args) {
		HashMap<JobNode, TIntArrayList> div = new HashMap<JobNode, TIntArrayList>();

		ArrayList<JobNode> serverList = new ArrayList<>();
		serverList.add(new JobNode(new Peer("1", ""), null, null));
		serverList.add(new JobNode(new Peer("2", ""), null, null));
		serverList.add(new JobNode(new Peer("3", ""), null, null));
		serverList.add(new JobNode(new Peer("4", ""), null, null));
		int[] data = new int[] { 1, 2, 3, 4 };
		System.out.println("Mapping " + data.length + " between "
				+ serverList.size());
		int count = 0;
		for (int i : data) {
			JobNode p = serverList.get(count);
			count = (count + 1) % serverList.size();
			TIntArrayList uList = div.get(p);
			if (uList == null) {
				uList = new TIntArrayList();
				div.put(p, uList);
			}
			uList.add(i);
		}
		System.out.println("Resulting list (size " + div.size() + ")");
		System.out.println(div);
	}
}
