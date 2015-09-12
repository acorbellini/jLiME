package edu.jlime.graphly.rec;

public class LPBeta implements Beta {

	private float beta;

	public LPBeta(float beta) {
		this.beta = beta;
	}

	@Override
	public float calc(int depth) {
		if (depth == 3)
			return beta;
		return 1f;
	}

	@Override
	public boolean mustSave(int i) {
		return i == 2 || i == 3;
	}

}
