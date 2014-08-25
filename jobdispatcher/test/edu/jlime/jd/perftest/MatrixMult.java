package edu.jlime.jd.perftest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.Client;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.jd.task.RoundRobinTask;

public class MatrixMult {

	private final class MatrixRM implements ResultListener<MultRes, Void> {

		private Integer count = 0;

		private final float[][] res;

		private MatrixRM(float[][] res) {
			this.res = res;
		}

		@Override
		public void onSuccess(MultRes result) {
			synchronized (this) {
				count++;
			}
			// System.out.println("Returned " + count + ".");
			res[result.getRow()] = result.getR();
		}

		@Override
		public Void onFinished() {
			return null;
		}

		@Override
		public void onFailure(Exception res) {
			res.printStackTrace();
		}
	}

	public static class MultRes implements Serializable {

		private static final long serialVersionUID = 6764712636922674748L;

		float[] r;

		int row;

		public MultRes(float[] r, int row) {
			super();
			this.r = r;
			this.row = row;
		}

		public int getRow() {
			return row;
		}

		public float[] getR() {
			return r;
		}
	}

	public static class RowColsMult implements Job<MultRes> {

		private static final long serialVersionUID = 2859740910452823627L;

		private float[] row;

		private int rindex;

		public RowColsMult(int rindex, float[] row) {
			this.row = row;
			this.rindex = rindex;
		}

		@Override
		public MultRes call(JobContext env, ClientNode peer) throws Exception {
			float[][] cols = (float[][]) env.waitFor("B");

			System.out.println("Multiplying " + cols.length + " cols by a "
					+ row.length + " sized row.");

			float[] res = new float[cols.length];
			int cont = 0;
			for (float[] c : cols) {
				res[cont++] = colMult(row, c);
			}
			return new MultRes(res, rindex);
		}

		private float colMult(float[] row, float[] c) {
			float res = 0;
			for (int i = 0; i < row.length; i++) {
				res += row[i] * c[i];
			}
			return res;
		}
	}

	float[][] testA = new float[][] { new float[] { 1, 2, 3 },
			new float[] { 4, 5, 6 }, new float[] { 7, 8, 9 },
			new float[] { 10, 11, 12 } };

	float[][] testB = new float[][] { new float[] { 10, 11, 12, 13 },
			new float[] { 14, 15, 16, 17 }, new float[] { 18, 19, 20, 21 } };

	public static void main(String[] args) throws Exception {
		new MatrixMult().MatrixMultTest();
	}

	public void MatrixMultTest() throws Exception {
		int rowsA = 2000;
		int colsA = 2000;

		int rowsB = 2000;
		int colsB = 2000;

		float[][] rowCol = new float[rowsA][];
		for (int i = 0; i < rowCol.length; i++) {
			rowCol[i] = new float[colsA];
		}

		float[][] colRow = new float[colsB][];
		for (int i = 0; i < colRow.length; i++) {
			colRow[i] = new float[rowsB];
		}

		for (int i = 0; i < rowsA; i++) {
			for (int j = 0; j < colsA; j++) {
				rowCol[i][j] = (float) (Math.random() * 1);
			}
		}
		for (int i = 0; i < colsB; i++) {
			for (int j = 0; j < rowsB; j++) {
				colRow[i][j] = (float) (Math.random() * 1);
			}
		}

		final List<RowColsMult> jobs = new ArrayList<>();
		for (int i = 0; i < rowCol.length; i++) {
			jobs.add(new RowColsMult(i, rowCol[i]));
		}

		final float[][] res = new float[rowsA][];

		System.out.println("Executing rr task.");

		Client cli = Client.build(1);

		RoundRobinTask<MultRes> rrTask = new RoundRobinTask<>(jobs,
				cli.getCluster());

		rrTask.set("B", colRow, true);
		long i = System.currentTimeMillis();

		MatrixRM list = new MatrixRM(res);
		rrTask.execute(list);
		long f = System.currentTimeMillis();

		System.out.println((f - i) / 1000);

		cli.close();
	}
}
