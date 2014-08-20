package edu.jlime.linkprediction;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import edu.jlime.collections.adjacencygraph.AdjacencyGraph;
import edu.jlime.collections.adjacencygraph.LocalAdjacencyGraph;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.adjacencygraph.query.Query;
import edu.jlime.collections.intintarray.db.StoreFactory;
import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;
import edu.jlime.jd.ClientCluster;
import edu.jlime.linkprediction.structural.SimilarityNeighboursSorensen;
import gnu.trove.list.array.TIntArrayList;

public class SimilarityTest {

	private Connection conn;

	private AdjacencyGraph graph;

	private int[] usersIds;

	private ClientCluster cluster;

	// private DEFClient cli;

	@Test
	public void similarityTest(String db, String output) throws Exception {
		try {
			Class.forName("org.h2.Driver");

			conn = DriverManager.getConnection("jdbc:h2:file:" + output, "sa",
			// ./results
					"");

			// cli = DEFClient.build(1);
			// cluster = cli.getCluster();
			graph = new LocalAdjacencyGraph(
					new StoreFactory(StoreType.H2).getStore(db
					// "D:/TwitterAdjacencyGraph/"
							, "DKVSDB"));

			// graph = new AdjacencyGraph(TwitterStoreConfig.getConfig(),
			// cluster,
			// new LocationMapper(TwitterStoreConfig.getConfig()
			// .getStoreName()));

			// graph = new AdyacencyGraph(new StoreConfig(StoreType.LEVELDB,
			// "/home/ale/TwitterDB", "TwitterLevelDB"),
			// cluster);

			// int[] f = graph.getUser(160763).neighbours().exec();
			// int[] ifol = graph.getUser(890121).neighbours().exec();
			// int[] res = IntArrayUtils.intersectArrays(ifol, f);
			// TIntHashSet set = new TIntHashSet();
			// set.addAll(graph.getUser(890121).neighbours().exec());
			// set.addAll(f);
			// float sim = res.length / (float) set.size();
			// System.out.println(sim);

			PreparedStatement stmt = conn
					.prepareStatement("CREATE TABLE IF NOT EXISTS USERSIMILARITY (USERID INTEGER , FOLLOWEEID INTEGER , TYPEOFSIMILARITY VARCHAR, SIMILARITY FLOAT, PRIMARY KEY(USERID, FOLLOWEEID, TYPEOFSIMILARITY));");
			stmt.execute();
			conn.commit();

			usersIds = scanIds("similarity.txt").toArray();
			int cont = 0;

			ExecutorService exec = Executors.newFixedThreadPool(10);
			final Semaphore sem = new Semaphore(10);

			for (final int user : usersIds) {
				System.out.println("List counter: " + cont++);
				sem.acquire();
				exec.execute(new Runnable() {

					@Override
					public void run() {
						try {
							processUser(user);
						} catch (Exception e) {
							e.printStackTrace();
						}
						sem.release();
					}

				});
			}
			exec.shutdown();
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			graph.close();
		}
		// cli.close();
	}

	private void processUser(int user) {

		ListQuery followees = graph.getUser(user).followees();// .filterBy(followeesIds);
		ListQuery followers = graph.getUser(user).followers();
		ListQuery neighbours = graph.getUser(user).neighbours();

		// if (!alreadyCalc(user, "COMMON_FOLLOWEES"))
		// try {
		// SimilarityTest.this.saveMap(followees
		// .foreach(new SimilarityFollowees(followees)),
		// "COMMON_FOLLOWEES", user);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// if (!alreadyCalc(user, "COMMON_FOLLOWERS"))
		// try {
		// SimilarityTest.this.saveMap(followees
		// .foreach(new SimilarityFollowers(followers)),
		// "COMMON_FOLLOWERS", user);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

		// if (!alreadyCalc(user, "COMMON_NEIGHBOURS"))
		// try {
		// SimilarityTest.this.saveMap(followees
		// .foreach(new SimilarityNeighbours(neighbours)),
		// "COMMON_NEIGHBOURS", user);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		//
		if (!alreadyCalc(user, "SORENSEN"))
			try {
				SimilarityTest.this.saveMap(followees
						.foreach(new SimilarityNeighboursSorensen(neighbours)),
						"SORENSEN", user);
			} catch (Exception e) {
				e.printStackTrace();
			}

		// try {
		// SimilarityTest.this.saveMap(followees.foreach(new
		// AdamicAdar(neighbours)), "F_ADAMICADAR", user);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

		// if (!alreadyCalc(user, "COMMON_NEIGHBOURS"))
		// try {
		// SimilarityTest.this.saveMap(neighbours
		// .foreach(new SimilarityNeighbours(neighbours)),
		// "COMMON_NEIGHBOURS", user);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

		// try {
		// SimilarityTest.this.saveMap(
		// neighbours.foreach(new SimilarityNeighboursSalton(
		// neighbours)), "COMMON_NEIGHBOURS_SALTON",
		// user);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		//
		// if (!alreadyCalc(user, "SORENSEN"))
		// try {
		// SimilarityTest.this.saveMap(neighbours
		// .foreach(new SimilarityNeighboursSorensen(
		// neighbours)),
		// "SORENSEN", user);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

		// try {
		// SimilarityTest.this.saveMap(neighbours
		// .foreach(new SimilarityNeighboursHPI(neighbours)),
		// "COMMON_NEIGHBOURS_HPI", user);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		//
		// try {
		// SimilarityTest.this.saveMap(neighbours
		// .foreach(new SimilarityNeighboursHDI(neighbours)),
		// "COMMON_NEIGHBOURS_HDI", user);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		//
		// try {
		// SimilarityTest.this.saveMap(neighbours
		// .foreach(new SimilarityNeighboursLHNI(neighbours)),
		// "COMMON_NEIGHBOURS_LHNI", user);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

		// try {
		// SimilarityTest.this.saveMap(neighbours.foreach(new
		// AdamicAdar(neighbours)), "ADAMICADAR", user);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
	}

	private boolean alreadyCalc(int user, String TypeOfSim) {
		PreparedStatement stmt = null;
		try {
			stmt = conn
					.prepareStatement("SELECT USERID FROM USERSIMILARITY WHERE USERID=? AND TYPEOFSIMILARITY=? LIMIT 1");
			stmt.setInt(1, user);
			stmt.setString(2, TypeOfSim);
			ResultSet rs = stmt.executeQuery();
			boolean ret = rs.first();
			return ret;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {

					e.printStackTrace();
				}
		}
		return false;
	}

	public static TIntArrayList scanIds(String file)
			throws FileNotFoundException {
		TIntArrayList ids = new TIntArrayList();
		Scanner scanner = new Scanner(new File(file));
		while (scanner.hasNext())
			ids.add(scanner.nextInt());
		scanner.close();
		return ids;
	}

	public void saveMap(Query<Map<Integer, Float>> forEachQuery,
			String TypeOfSim, Integer user) throws Exception {
		System.out.println("Processing user " + user + " with similarity "
				+ TypeOfSim);
		Map<Integer, Float> map;
		try {
			map = forEachQuery.query();
		} catch (Exception e) {
			throw e;
		}
		// ArrayList<Integer> keys = new ArrayList<>(map.keySet());
		// Collections.sort(keys);
		// int count = 0;
		// StringBuffer buffer = new StringBuffer();
		// for (Integer k : keys) {
		// if (count == 0)
		// buffer.append("\n");
		//
		// count = ++count % 8;
		//
		// buffer.append(k + "=>" + new
		// DecimalFormat("#.###").format(map.get(k)) + "\t\t");
		// }
		// System.out.println(buffer.toString());
		// System.out.println(StringUtils.readableTime(forEachQuery.getQueryTime()));
		StringBuilder insert = new StringBuilder();
		boolean first = true;
		for (Integer uid : map.keySet()) {
			if (first)
				first = false;
			else
				insert.append(",");

			insert.append(" (" + user + "," + uid + ",'" + TypeOfSim + "',"
					+ map.get(uid) + ") ");

		}
		try {
			PreparedStatement stmt = conn
					.prepareStatement("MERGE INTO USERSIMILARITY VALUES "
							+ insert.toString());
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		conn.commit();

	}

	public static void main(String[] args) throws Exception {
		new SimilarityTest().similarityTest(args[0], args[1]);
	}

}
