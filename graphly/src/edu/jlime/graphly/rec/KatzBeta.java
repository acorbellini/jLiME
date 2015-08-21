package edu.jlime.graphly.rec;

public class KatzBeta implements BetaCalc {

	private float beta;

	public KatzBeta(float beta) {
		this.beta = beta;
	}

	@Override
	public float calc(int depth) {
		return (float) Math.pow(beta, depth);
	}

	@Override
	public boolean mustSave(int i) {
		return true;
	}

}
