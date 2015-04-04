package edu.jlime.linkprediction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.junit.Test;

import edu.jlime.collections.adjacencygraph.RemoteAdjacencyGraph;
import edu.jlime.collections.adjacencygraph.mappers.RoundRobinMapper;
import edu.jlime.collections.adjacencygraph.query.UserQuery;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.client.Client;

public class AddISToResults {

	private ClientCluster cluster;

	private Connection conn;

	private RemoteAdjacencyGraph graph;

	private int[] usersIds;

	@Test
	public void addIS() throws Exception {
		Client cli = Client.build(1);
		cluster = cli.getCluster();
		graph = new RemoteAdjacencyGraph(new StoreConfig(StoreType.LEVELDB,
				"C:/TwitterAdjacencyGraph", "Twitter1stAnd2ndLevel"), cluster,
				new RoundRobinMapper());

		usersIds = SimilarityTest.scanIds("D:/1stLayer.txt").toArray();
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				"ahoraTienenIS.txt")));
		// ESTO AGREGA A LOS RESULTADOS

		// ESTE AGREGA A USERIS
		for (int u : usersIds) {
			System.out.println("Calulating IS for user " + u);
			UserQuery userQ = graph.getUser(u);
			int followers = userQ.followers().size().query();
			int followees = userQ.followees().size().query();
			Float is = followers / (float) (followees + followers);
			writer.write(u + ";" + is + "\n");
		}
		writer.close();
		graph.close();
		cli.close();
	}

	public void addToDBSelectedUsers(int u, Float is) throws SQLException,
			ClassNotFoundException {
		Class.forName("org.h2.Driver");

		conn = DriverManager.getConnection("jdbc:h2:file:./results", "sa", "");

		System.out.println("Adding IS for user " + u);
		try {
			PreparedStatement stmt = conn
					.prepareStatement("MERGE INTO SELECTEDUSERIS VALUES(?,?) ");
			stmt.setInt(1, u);
			stmt.setFloat(2, is);
			stmt.execute();

			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void addToDB(int u, Map<Integer, Float> map)
			throws ClassNotFoundException, SQLException {
		Class.forName("org.h2.Driver");

		conn = DriverManager.getConnection("jdbc:h2:file:./results", "sa", "");

		System.out.println("Adding IS for user " + u);
		try {
			for (Integer uid : map.keySet()) {
				PreparedStatement stmt = conn
						.prepareStatement("MERGE INTO FOLLOWEEIS SET VALUES(?,?) ");
				stmt.setInt(1, uid);
				stmt.setFloat(2, map.get(uid));
				stmt.execute();
			}

			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		new AddISToResults().addIS();
	}
}
