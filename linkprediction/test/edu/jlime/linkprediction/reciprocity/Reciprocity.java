package edu.jlime.linkprediction.reciprocity;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import edu.jlime.client.Client;
import edu.jlime.collections.adjacencygraph.AdjacencyGraph;
import edu.jlime.collections.adjacencygraph.RemoteAdjacencyGraph;
import edu.jlime.collections.adjacencygraph.mappers.LocationMapper;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.jd.JobCluster;
import edu.jlime.linkprediction.SimilarityTest;
import edu.jlime.linkprediction.TwitterStoreConfig;

public class Reciprocity {

	private static Connection conn;

	private static Client cli;

	private static JobCluster cluster;

	private static AdjacencyGraph graph;

	private static int[] usersIds;

	public static void main(String[] args) throws Exception {
		Class.forName("org.h2.Driver");

		conn = DriverManager.getConnection("jdbc:h2:file:./results", "sa", "");

		PreparedStatement create = conn
				.prepareStatement("CREATE TABLE IF NOT EXISTS PUBLIC.RECIPROCITY ("
						+ "USERID VARCHAR_IGNORECASE(8) PRIMARY KEY,FOLLOWEES VARCHAR_IGNORECASE(6),	"
						+ "FOLLOW_BACK VARCHAR_IGNORECASE(6)" + ")");
		create.execute();
		conn.commit();

		cli = Client.build(8);
		cluster = cli.getCluster();

		graph = new RemoteAdjacencyGraph(TwitterStoreConfig.getConfig(),
				cluster, new LocationMapper(TwitterStoreConfig.getConfig()
						.getStoreName()));

		usersIds = SimilarityTest.scanIds("userIds.txt").toArray();

		for (final int u : usersIds) {
			System.out.println("User: " + u);
			ListQuery uQuery = graph.getUser(u);
			int reciprocous = uQuery.followees().intersect(uQuery.followers())
					.size().query();
			Integer followeesSize = uQuery.followees().size().query();
			try {
				saveReciprocity(u, followeesSize, reciprocous);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		cli.close();

	}

	private static synchronized void saveReciprocity(int u, int f, int r)
			throws SQLException {
		PreparedStatement stmt = conn
				.prepareStatement("MERGE INTO RECIPROCITY VALUES(?,?,?);");
		stmt.setInt(1, u);
		stmt.setInt(2, f);
		stmt.setInt(3, r);
		stmt.execute();

		conn.commit();
	}
}
