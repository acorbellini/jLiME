package edu.jlime.linkprediction;

import java.io.File;
import java.util.Scanner;

import edu.jlime.collections.adjacencygraph.RemoteAdjacencyGraph;
import edu.jlime.collections.adjacencygraph.mappers.LocationMapper;
import edu.jlime.collections.adjacencygraph.query.UserQuery;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.client.Client;

public class GetIS {
	public static void main(String[] args) throws Exception {
		Client client = Client.build(8);
		ClientCluster cluster = client.getCluster();
		System.out.println("Creating adyacency graph.");
		StoreConfig config = TwitterStoreConfig.getConfig();
		RemoteAdjacencyGraph graph = new RemoteAdjacencyGraph(config, cluster,
				new LocationMapper(config.getStoreName()));

		Scanner scanner = new Scanner(new File(args[0]));
		while (scanner.hasNext()) {
			int uid = scanner.nextInt();
			UserQuery user = graph.getUser(uid);
			Integer countFollowees = user.followees().size().query();
			Integer countFollowers = user.followers().size().query();
			System.out
					.println(uid
							+ " "
							+ countFollowees
							+ " "
							+ countFollowers
							+ " "
							+ (countFollowers / (float) (countFollowees + countFollowers)));
		}
	}
}
