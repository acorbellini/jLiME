package edu.jlime.collections.twittergraph;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.collections.intintarray.db.StoreFactory;
import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.list.array.TIntArrayList;

public class TwitterGraphTest {

	public static void main(String[] args) throws Exception {
		System.out.println(Integer.MAX_VALUE);

		// Load a twitter adjacency graph of the type u->[followees][followers]
		// JobCluster cluster = DEFClient.build(1).getCluster();
		// BundlerClient graph = new BundlerClient(new StoreConfig(
		// StoreType.LEVELDB, ,
		// ), 10000, cluster);

		Store graph = (Store) new StoreFactory(StoreType.H2).getStore(
				"D:/TwitterAdjacencyGraph/", "twitterGraph");

		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(
				new File(args[0])));

		long u;
		int cont = 0;
		while ((u = readLong(bis)) != -1) {
			System.out.println((cont++) + ":" + u);
			int followeesSize = readInt(bis) / 8;
			TIntArrayList list = new TIntArrayList(2048);
			for (int i = 0; i < followeesSize; i++) {
				list.add((int) readLong(bis));
			}
			int followersSize = readInt(bis) / 8;
			TIntArrayList list2 = new TIntArrayList();
			for (int i = 0; i < followersSize; i++) {
				list2.add((int) readLong(bis));
			}

			graph.store((int) u, list.toArray());
			graph.store((int) -u, list2.toArray());
			if (cont % 1000 == 0)
				graph.commit();
		}
		graph.close();
	}

	private static int readInt(BufferedInputStream bis) throws IOException {
		byte[] l = new byte[4];
		l[0] = (byte) bis.read();
		l[1] = (byte) bis.read();
		l[2] = (byte) bis.read();
		l[3] = (byte) bis.read();
		return DataTypeUtils.byteArrayToInt(l);
	}

	private static long readLong(BufferedInputStream bis) throws IOException {
		byte[] l = new byte[8];
		l[0] = (byte) bis.read();
		if (l[0] == -1)
			return -1;
		l[1] = (byte) bis.read();
		l[2] = (byte) bis.read();
		l[3] = (byte) bis.read();
		l[4] = (byte) bis.read();
		l[5] = (byte) bis.read();
		l[6] = (byte) bis.read();
		l[7] = (byte) bis.read();
		return DataTypeUtils.byteArrayToLong(l, 0);
	}
}
