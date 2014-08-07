package edu.jlime.collections.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import edu.jlime.client.Client;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;

public class TopUsersExtractor {

	public static void main(String[] args) throws Exception {

		PersistentIntIntArrayMap client = new PersistentIntIntArrayMap(
				new StoreConfig(StoreType.LEVELDB, "./twitterStore", "twitter"),
				Client.build().getCluster());

		Thread.sleep(2000);

		BufferedReader reader = new BufferedReader(new FileReader(new File(
				args[0])));

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				args[1])));

		String line = "";
		int count = 0;
		while ((line = reader.readLine()) != null) {
			int[] followees = client.get(new Integer(line));
			int[] followers = client.get(new Integer("-" + line));
			int followeesCount = followees == null ? 0 : followees.length;
			int followersCount = followers == null ? 0 : followers.length;

			float IS = (followersCount - followeesCount)
					/ (float) (followersCount + followeesCount);

			IS = (IS + 1) / 2;

			if (IS < new Float(args[2])
					&& followeesCount > new Integer(args[3])) {
				writer.write(line + " " + followeesCount + " " + followersCount
						+ " " + IS + "\r\n");
				count++;
			}
		}
		reader.close();
		writer.close();
		System.out.println("Se encontraron " + count + " usuarios.");
	}
}
