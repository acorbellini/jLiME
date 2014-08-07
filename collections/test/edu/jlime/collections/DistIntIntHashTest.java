package edu.jlime.collections;

import org.junit.Test;

import edu.jlime.client.Client;
import edu.jlime.collections.intint.DistIntIntHashtable;

public class DistIntIntHashTest {

	@Test
	public void simpleTest() throws Exception {
		DistIntIntHashtable hash = new DistIntIntHashtable(Client.build()
				.getCluster());

		hash.putOrAdd(1, 5);
		hash.putOrAdd(1, 5);
		hash.putOrAdd(1, 5);
		hash.putOrAdd(1, 5);

		hash.putOrAdd(2, 1);
		hash.putOrAdd(2, 2);
		hash.putOrAdd(2, 3);
		hash.putOrAdd(2, 4);

		hash.putOrAdd(3, 15);
		hash.putOrAdd(3, 15);

		hash.putOrAdd(4, 20);
		hash.putOrAdd(4, 20);

		hash.putOrAdd(5, 30);
		hash.putOrAdd(5, 60);

		hash.putOrAdd(7, 100);
		hash.putOrAdd(7, 10);

		for (int[] is : hash) {
			System.out.println(is[0] + "," + is[1]);
		}
	}
}
