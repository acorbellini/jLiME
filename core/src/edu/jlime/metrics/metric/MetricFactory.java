package edu.jlime.metrics.metric;

import edu.jlime.metrics.meters.Accumulator;
import edu.jlime.metrics.meters.Counter;
import edu.jlime.metrics.meters.Gauge;
import edu.jlime.metrics.meters.Meter;
import edu.jlime.metrics.meters.MetricSet;
import edu.jlime.metrics.meters.Simple;

public abstract class MetricFactory<T> {

	public abstract Metric<T> create();

	public static MetricFactory<Float> gaugeFactory = new MetricFactory<Float>() {

		@Override
		public Metric<Float> create() {
			return new Gauge();
		}
	};

	public static MetricFactory<Object> simpleFactory = new MetricFactory<Object>() {

		@Override
		public Metric<Object> create() {
			return new Simple();
		}
	};

	public static MetricFactory<Float> meterFactory = new MetricFactory<Float>() {

		@Override
		public Metric<Float> create() {
			return new Meter();
		}
	};

	public static MetricFactory<Float> accumulatorFactory = new MetricFactory<Float>() {

		@Override
		public Metric<Float> create() {
			return new Accumulator();
		}
	};

	public static MetricFactory<Float> counterFactory = new MetricFactory<Float>() {

		@Override
		public Metric<Float> create() {
			return new Counter();
		}
	};

	public static MetricFactory<Object> setFactory = new MetricFactory<Object>() {

		@Override
		public Metric<Object> create() {
			return new MetricSet();
		}
	};

}