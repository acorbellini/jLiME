package edu.jlime.collections.querytest;

import java.util.Iterator;

import org.junit.Test;

import edu.jlime.client.Client;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.collections.adjacencygraph.RemoteAdjacencyGraph;
import edu.jlime.collections.adjacencygraph.mappers.MapperFactory;
import edu.jlime.collections.adjacencygraph.query.RemoteListQuery;
import edu.jlime.collections.adjacencygraph.query.TopQuery;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;
import edu.jlime.jd.JobCluster;
import edu.jlime.util.StringUtils;

public class QueryTest {

	private JobCluster cluster;

	@Test
	public void queryTest() throws Exception {
		Client client = Client.build(8);
		cluster = client.getCluster();

		// StoreConfig config = new StoreConfig(StoreType.LEVELDB,
		// "./twitterStore", "twitter");
		StoreConfig config = new StoreConfig(StoreType.LEVELDB,
				"/home/acorbellini/TwitterDB", "TwitterLevelDB");

		// PersistentIntIntArrayMap map = new PersistentIntIntArrayMap(config,
		// cluster);

		// Mapper mapper = MapperFactory.cpuCriteria();
		// Mapper mapper = new LocationMapper(config.getStoreName());
		Mapper mapper = MapperFactory.memCriteria();
		RemoteAdjacencyGraph graph = new RemoteAdjacencyGraph(config, cluster,
				mapper);

		RemoteListQuery followees = graph.getUser(12).followees();

		TopQuery query = followees.followers().remove(followees)
				.countFollowees().remove(followees).top(10);

		printTop(query);

		// TopQuery queryWithoutRemove =
		// graph.followees(12).followers().countFollowees().top(10);
		//
		// printTop(queryWithoutRemove);
		//
		// TopQuery query3 =
		// graph.followees(12).followers().remove(graph.followees(12)).countFollowees()
		// .remove(graph.followees(12)).top(10);
		//
		// printTop(query3);
		graph.close();
		client.close();
	}

	public void printTop(TopQuery query) throws Exception {
		Iterator<int[]> it = query.query().iterator();

		while (it.hasNext()) {
			int[] pair = it.next();
			System.out.println(pair[0] + "->" + pair[1]);
		}

		System.out.println(StringUtils.readableTime(query.getQueryTime()));
	}
}
