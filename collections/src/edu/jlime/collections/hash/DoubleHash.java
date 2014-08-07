package edu.jlime.collections.hash;

public class DoubleHash {

	SimpleIntIntHash[] hashes = new SimpleIntIntHash[50];

	public int get(int k) throws Exception {
		return hashes[hash(k)].get(k);

	}

	public int hash(int k) {
		return (int) Math.abs((k * 5700357409661598721L) % hashes.length);
		// return k % MAX_BUCKETS;
	}

	public void put(int k, int v) {
		if (hashes[hash(k)] == null)
			hashes[hash(k)] = new SimpleIntIntHash();
		hashes[hash(k)].put(k, v);
	}

}
