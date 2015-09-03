package edu.jlime.graphly.traversal;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.rec.CustomStep;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.Repeat;
import edu.jlime.graphly.rec.VertexFilter;
import edu.jlime.graphly.traversal.RepeatStep.RepeatSync;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.graphly.traversal.count.ParallelStep;
import edu.jlime.graphly.traversal.each.EachStep;
import edu.jlime.graphly.traversal.each.ForEach;
import edu.jlime.jd.ClientNode;
import gnu.trove.set.hash.TLongHashSet;

public class GraphlyTraversal implements Serializable {

	Map<String, Object> vars = new HashMap<>();

	List<Step> steps = new ArrayList<>();

	int curr = 0;

	private TraversalResult currres;

	private GraphlyGraph g;

	private boolean printSteps = false;
	private long lastexec = -1;

	private String graphID;

	public GraphlyTraversal(long[] ids, GraphlyGraph g) {
		this.currres = new VertexResult(ids);
		this.g = g;
	}

	public GraphlyTraversal(TLongHashSet ids, GraphlyGraph graph) {
		this.currres = new VertexResult(ids);
		this.g = graph;
	}

	public GraphlyTraversal setPrintSteps(boolean printSteps) {
		this.printSteps = printSteps;
		return this;
	}

	public TraversalResult submit(ClientNode c) throws Exception {
		return c.exec(new TraversalJob(this));
	}

	public TraversalResult next() throws Exception {
		Step step = steps.get(curr++);
		if (printSteps) {
			Logger log = Logger.getLogger(GraphlyTraversal.class);
			if (lastexec < 0)
				lastexec = System.currentTimeMillis();

			log.info("Executing step " + curr + "("
					+ (System.currentTimeMillis() - lastexec) / 1000
					+ " sec. ):" + step.toString());
		}
		this.currres = step.exec(currres);
		return this.currres;
	}

	public TraversalResult exec() throws Exception {
		for (int i = curr; i < steps.size(); i++) {
			next();
		}
		return this.currres;
	}

	public GraphlyTraversal out() {
		return to(Dir.OUT);
	}

	public GraphlyTraversal to(Dir dir, int max_edges) {
		addStep(new VertexStep(dir, max_edges, this));
		return this;
	}

	public GraphlyTraversal addStep(Step vertexStep) {
		steps.add(vertexStep);
		return this;
	}

	public Object get(String k) {
		return vars.get(k);
	}

	public GraphlyTraversal set(String k, Object v) {
		vars.put(k, v);
		return this;
	}

	public GraphlyTraversal in() {
		return to(Dir.IN);
	}

	public GraphlyGraph getGraph() {
		return g;
	}

	public GraphlyTraversal filter(long[] first) {
		addStep(new FilterStep(first, this));
		return this;
	}

	public GraphlyTraversal count(Dir dir, int max_edges, String[] filters) {
		// addStep(new ParallelStep(new CountStep(dir, max_edges, this), this,
		// 2,
		// new CountMerger()));
		addStep(new CountStep(dir, max_edges, filters, this));
		return this;
	}

	public GraphlyTraversal countOut() {
		return countOut(Integer.MAX_VALUE);
	}

	public GraphlyTraversal countOut(int max_edges) {
		return count(Dir.OUT, max_edges);
	}

	private GraphlyTraversal count(Dir dir, int max_edges) {
		return this.count(dir, max_edges, new String[] {});
	}

	public <T extends CustomTraversal> T as(Class<T> c) throws Exception {
		Constructor<T> cons = c.getConstructor(GraphlyTraversal.class);
		return cons.newInstance(this);
	}

	public Object getCurrres() {
		return currres;
	}

	public GraphlyTraversal filterBy(String... k) {
		addStep(new VarFilterStep(k, this, true));
		return this;
	}

	public GraphlyTraversal save(String k) {
		addStep(new SaveStep(k, this));
		return this;
	}

	public GraphlyTraversal traverse(int max_edges, Dir... dirs) {
		return this.traverse(new String[] {}, max_edges, dirs);
	}

	public GraphlyTraversal traverse(String[] filters, int max_edges,
			Dir... dirs) {
		if (dirs.length == 0)
			return this;

		for (int i = 0; i < dirs.length; i++) {
			to(dirs[i], max_edges);
			filterBy(filters);
		}
		return this;
	}

	public GraphlyTraversal traverseCount(Dir... dirs) {
		return traverseCount(new String[] {}, Integer.MAX_VALUE, dirs);
	}

	public GraphlyTraversal traverseCount(String[] filters, int max_edges,
			Dir... dirs) {
		for (int i = 0; i < dirs.length; i++) {
			count(dirs[i], max_edges, filters);
		}
		return this;
	}

	public GraphlyTraversal randomOut() {
		return random(Dir.OUT, new long[] {});
	}

	public GraphlyTraversal random(Dir dir, long[] subset) {
		addStep(new RandomStep(dir, subset, this));
		return this;
	}

	public <T> GraphlyTraversal each(int steps, String key, ForEach<T> forEach) {
		addStep(new EachStep(key, steps, forEach, this));
		return this;
	}

	public GraphlyTraversal join(String from, String to, Join join) {
		addStep(new JoinStep(from, to, join, this));
		return this;
	}

	public GraphlyTraversal filterNeg(long[] before) {
		addStep(new FilterStep(before, this, true));
		return this;
	}

	public GraphlyTraversal repeat(int steps, Repeat<long[]> rfunc,
			RepeatSync<long[]> sync) {
		addStep(new RepeatStep(steps, rfunc, sync, this));
		return this;
	}

	public GraphlyTraversal traverse(String[] strings, Dir[] copyOfRange) {
		return traverse(strings, Integer.MAX_VALUE, copyOfRange);
	}

	public GraphlyTraversal to(Dir dir) {
		return to(dir, -1);
	}

	public GraphlyTraversal filter(VertexFilter customFilter) {
		addStep(new CustomFilterStep(customFilter, this));
		return this;
	}

	public GraphlyTraversal customStep(CustomFunction f) {
		addStep(new CustomStep(this, f));
		return this;
	}

	public GraphlyTraversal top(int top) {
		addStep(new TopStep(top, this));
		return this;

	}

	public GraphlyTraversal add(long... users) {
		addStep(new AddVertexStep(this, users));
		return this;
	}

	public GraphlyTraversal intersect(Dir dir) {
		addStep(new IntersectStep(dir, this));
		return this;
	}

	public GraphlyTraversal size() {
		addStep(new SizeStep());
		return this;
	}

	public GraphlyTraversal expand(Dir dir, int max_edges) {
		addStep(new VertexStep(dir, max_edges, true, this));
		return this;
	}

	public GraphlyTraversal traverseGraphCount(String countk, String[] filters,
			int max_edges, Dir... dirs) {
		for (int i = 0; i < dirs.length; i++) {
			graphcount(countk, i == 0 ? null : filters, dirs[i], max_edges,
					i != dirs.length - 1);
		}
		return this;
	}

	public GraphlyTraversal graphcount(String countk, String[] filters,
			Dir dir, int max_edges, boolean returnVertices) {
		addStep(new GraphCountStep(dir, filters, max_edges, this, countk,
				returnVertices));
		return this;
	}

	public GraphlyTraversal mark(String string) {
		addStep(new MarkStep(string, this));
		return this;
	}
}
