package edu.jlime.collections.persistent;

import edu.jlime.client.Client;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.junit.Test;

public class GetAndPutTest {

	Logger log = Logger.getLogger(GetAndPutTest.class);

	@Test
	public void testGetAndPut() throws Exception {
		int[] data = new int[] { 14105522, 15999743, 1657131, 16169115,
				16994966, 16247161, 15689690, 13250922, 6757022, 1937971,
				14582556, 16436780, 16169649, 15893497, 13838982, 15302271,
				15211246, 16210610, 8574482, 14067464, 16707074, 15118852,
				17099277, 15321947, 17047435, 987541, 14347411, 16017289,
				15554725, 14764691, 6071832, 15947756, 15588801, 15574772,
				9008842, 15836408, 14983931, 15989744, 15226747, 14681812,
				17011706, 16642547, 15140194, 14827694, 2103771, 16326828,
				14944810, 15992633, 14747515, 15223117, 15932648, 7830812,
				4663341, 14318809, 15948197, 16079846, 14162994, 16363972,
				14172322, 735233, 14549944, 16596073, 14798905, 14633068,
				15312211, 7027452, 16422217, 10032542, 15586489, 16114164,
				14488143, 15686001, 4501921, 15317824, 14588531, 14928478,
				16681989, 16620309, 15822704, 11179282, 16576237, 15536881,
				14388281, 755622, 9500122, 15120287, 14451428, 14953683,
				15384012, 15751590, 14929618, 15358077, 14335619, 15665948,
				7649512, 14310129, 14306336, 8873882, 14828685, 15236569,
				14800894, 14188267, 15126529, 16830780, 15301178, 15313723,
				14728164, 15237715, 14315930, 3580961, 14590673, 14085664,
				14196518, 17015136, 15699210, 14189222, 15054995, 5810222,
				15044481, 16906203, 14195846, 14752807, 16878714, 8675562,
				15959367, 5973372, 14696825, 10710532, 16855116, 15555841,
				14263128, 15092100, 7097322, 17068177, 4678511, 14267733,
				14264997, 8788202, 15668633, 15919110, 16472956, 15553838,
				15511086, 4457061, 16135147, 14140571, 16833855, 14764436,
				15334803, 5540702, 15291937, 7313302, 14751450, 15744562,
				17064358, 17049929, 6185712, 10814252, 11194172, 14930711,
				1780881, 5834022, 14961732, 11064112, 11021342, 16037977,
				14573342, 8763442, 1827931, 14832213, 3388541, 637753,
				16911942, 15505804, 16581979, 15209892, 16356324, 5658362,
				11093062, 7930172, 16583813, 15331676, 15697765, 14967860,
				13612022, 14688037, 15986468, 15080874, 15594822, 15698365,
				5873842, 16206444, 14151376, 14599083, 17041402, 16439421,
				16925659, 4178231, 15876028, 8599162, 7500142, 15403731,
				14425188, 15504154, 15154908, 14062556, 11935062, 17052759,
				17039904, 10404622, 5902352, 16526859, 14155545, 14819817,
				15252558, 15490327, 16091157, 7351742, 16687827, 7058382,
				15971737, 14470723, 15676557, 13834452, 14837401, 16236449,
				15845696, 16608640, 14851935, 15152856, 16210273, 15611269,
				6460932, 16582093, 14977131, 14998471, 14189585, 14878556,
				2875441, 14588473, 16659957, 15106450, 16396274, 16263351,
				16481034, 9653072, 15309193, 15697506, 14703803, 16399446,
				16406455, 9490952, 11184872, 17130669, 16078329, 15038885,
				14768489, 15212808, 9733972, 9838142, 16752967, 15667561,
				15048081, 15315240, 12642972, 16806147, 6702362, 15089492,
				16803327, 11381872, 17068660, 16474151, 15186156, 11676072,
				11836852, 15111246, 6834662, 15376048, 6811622, 14549087,
				14911331, 15824604, 7846992, 8176592, 14952705, 15142087,
				7756422, 13208262, 16325563, 2147541, 16630301, 15965323,
				1524801, 16503589, 14200990, 3075471, 10882082, 17006836,
				4856791, 10821762, 15390715, 11587392, 15632361, 15881400,
				11930152, 8783502, 16739937, 10661222, 7403342, 16653869,
				14887890, 16660707, 14519530, 14238120, 15328557, 15247459,
				12973412, 14302234, 14259290, 15823003, 10513882, 16057299,
				6566282, 2295741, 3457871, 16692549, 11164252, 5366812,
				14302570, 16434603, 15058002, 15670334, 11850642, 15757839,
				9809972, 14432689, 5464902, 15913365, 17023996, 14679896,
				16178161, 14600348, 16556091, 16961010, 10676522, 14323346,
				14527492, 15052392, 13982032, 15457970, 16337113, 16274648,
				15422852, 1784341, 16393196, 13805282, 16894705, 15053290,
				15992512, 14261479, 11928752, 6818262, 15250555, 14158106,
				794158, 15275373, 10697522, 16917706, 15779170, 17092557,
				16029131, 14851287, 15410332, 15682739, 16036631, 16196030,
				14884924, 9837192, 16302804, 16902309, 7838012, 16335402,
				15058981, 16117504, 15459813, 7225182, 6069132, 16371754,
				16752236, 16972853, 15505190, 3971681, 14716929, 14926877,
				17112155, 16015701, 16371950, 15251907, 1365921, 14993455,
				7139902, 15522109, 12578232, 16905602, 14059230, 16950066,
				14452822, 5499992, 16521719, 16610801, 14609744, 11257942,
				14915263, 13849522, 14841083, 7138082, 14102775, 15873466,
				16890212, 11202012, 14196594, 15625978, 9424982, 15657170,
				14137235, 9647152, 14527241, 15757670, 7374092, 15453121,
				16530737, 14789340, 16969387, 989231, 17060888, 15075676,
				15482248, 15496841, 14390965, 15808184, 13371352, 11172992,
				16268801, 16348917, 16160185, 14068496, 9425012, 16714614,
				16495836, 14076823, 5437842, 6322472, 16483893, 15852447,
				14135318, 7499952, 16580704, 14731158, 10260132, 16327773,
				15452654, 15735917, 16160123, 15087234, 15339178, 15041559,
				16409289, 14370253, 15120597, 15360084, 14692814, 15108759,
				8360732, 16245636, 14978123, 16996940, 15741191, 17053938,
				14254582, 17114209, 15248457, 15179748, 14257001, 14483652,
				14452905, 9547392, 16069551, 11514672, 14757775, 14938270,
				15667033, 14526326, 16009648, 14971024, 17027360, 15928119,
				16175888, 16650399, 15100341, 823655 };

		TIntObjectHashMap<int[]> orig = new TIntObjectHashMap<int[]>();
		log.info("Size of data : " + data.length);

		for (int i : data) {
			int size = (int) (Math.random() * 100 + 1);
			int[] list = new int[size];
			for (int j = 0; j < list.length; j++) {
				list[j] = data[(int) (Math.random() * data.length)];
			}
			Arrays.sort(list);
			orig.put(i, list);
		}
		PersistentIntIntArrayMap c = new PersistentIntIntArrayMap(
				new StoreConfig(StoreType.LEVELDB, "test", "testDB"), Client
						.build().getCluster());
		c.batchSet(10, orig);

		// for (Entry<Integer, int[]> entry : orig.entrySet()) {
		// long before = Calendar.getInstance().getTimeInMillis();
		// c.set(entry.getKey(), entry.getValue());
		// long after = Calendar.getInstance().getTimeInMillis();
		// log.info("Time to set : " + ((after - before) / (float) 1000)
		// + " sec");
		// }

		// c.set(orig);

		// TIntObjectHashMap<int[]> subres = c.batchGet(data, new GetProcedure()
		// {
		// public void process(TIntObjectHashMap<int[]> subres) {
		//
		// }
		// });
		TIntObjectHashMap<int[]> res = c.get(data);
		for (int i : data) {
			int[] savedVal = res.get(i);
			int[] origVal = orig.get(i);
			assert savedVal.length != origVal.length;
			for (int j = 0; j < origVal.length; j++) {
				assert savedVal[j] == origVal[j];
			}
		}

		log.info("Finalizo correctamente.");
		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		new GetAndPutTest().testGetAndPut();
	}
}
