package edu.jlime.graphly.util;

public class Pair<L, R> {
	L left;
	R right;

	public Pair(L left, R right) {
		super();
		this.left = left;
		this.right = right;
	}

	public L getKey() {
		return left;
	}

	public R getValue() {
		return right;
	}

	public static <L, R> Pair<L, R> build(L k, R v) {
		return new Pair<>(k, v);
	}

}
