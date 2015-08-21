package edu.jlime.util;

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

	@Override
	public String toString() {
		return "Pair [left=" + left + ", right=" + right + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((left == null) ? 0 : left.hashCode());
		result = prime * result + ((right == null) ? 0 : right.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (left == null) {
			if (other.left != null)
				return false;
		} else if (!left.equals(other.left))
			return false;
		if (right == null) {
			if (other.right != null)
				return false;
		} else if (!right.equals(other.right))
			return false;
		return true;
	}

}
