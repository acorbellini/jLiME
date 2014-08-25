package edu.jlime.linkprediction.querytest;

import org.junit.Test;

import edu.jlime.collections.adjacencygraph.AdjacencyGraph;
import edu.jlime.collections.adjacencygraph.RemoteAdjacencyGraph;
import edu.jlime.collections.adjacencygraph.mappers.LocationMapper;
import edu.jlime.collections.util.IntArrayUtils;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.client.Client;
import edu.jlime.linkprediction.TwitterStoreConfig;

public class QueryTest {

	private ClientCluster cluster;

	private AdjacencyGraph graph;

	@Test
	public void queryTest() throws Exception {

		System.setProperty("java.net.preferIPv4Stack", "true");

		cluster = Client.build(8).getCluster();

		// PersistentIntIntArrayMap map = new
		// PersistentIntIntArrayMap(TwitterStoreConfig.getConfig(), cluster);
		// map.set(31523933, new int[] { 639643, 641433, 1436461, 1471021,
		// 3014561, 4404141, 5203191, 9671282, 12574492,
		// 12860612, 14058735, 14876536, 15025119, 15648965, 15665583, 15808708,
		// 16100711, 16134731, 16341388,
		// 16579337, 16605320, 16952499, 16980410, 17002699, 17087575, 17460688,
		// 17460742, 18424102, 18646356,
		// 19087497, 19139073, 19395093, 20842473, 21146714, 21351971, 21386236,
		// 21589392, 21606687, 21608939,
		// 22121029, 22144364, 22156592, 22741229, 24144216, 24391679, 24926710,
		// 25576592, 26875118, 27072070,
		// 27205626, 27363788, 28407303, 28533323, 28881439, 29047058, 29081165,
		// 29348487, 30821168, 31214725,
		// 33066789, 33424689, 33437533, 34225445, 36279501, 36405354, 36468864,
		// 37469227, 37688395, 37689986,
		// 37690397, 37698278, 37699305, 37699567, 37701261, 37702079, 39407092,
		// 39415484, 40116295, 40625029,
		// 40634780, 40829024, 41025940, 41027226, 41325313, 41909212, 42001143,
		// 42048909, 42065072, 42258792,
		// 42280332, 42845520, 43155617, 43504631, 44021283, 45678476, 47473077,
		// 48255881, 48472573, 49848214,
		// 50031873, 53254881, 53997801, 54051807, 54493958, 55293359, 55654116,
		// 56147462, 56559465, 58029111,
		// 58521525, 59507971, 59924814, 60834607, 61054158 });

		graph = new RemoteAdjacencyGraph(TwitterStoreConfig.getConfig(),
				cluster, new LocationMapper(TwitterStoreConfig.getConfig()
						.getStoreName()));

		// graph = new AdyacencyGraph(new StoreConfig(StoreType.LEVELDB,
		// "/home/ale/TwitterDB", "TwitterLevelDB"), cluster);
		int[] n = graph.getUser(31523933).followees().query();
		int[] n2 = graph.getUser(18902263).followees().query();

		System.out.println(n.length);
		System.out.println(n2.length);
		int intersect = IntArrayUtils.intersectCount(n, n2);
		float union = (float) IntArrayUtils.unionCount(n, n2);
		System.out.println(intersect / union);

	}
}
