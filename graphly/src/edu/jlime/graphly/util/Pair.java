package edu.jlime.graphly.util;

import java.io.Serializable;

public class Pair<L, R> implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3575315521739357724L;
	public L left;
	public R right;

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
