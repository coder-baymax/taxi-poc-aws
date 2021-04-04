package cool.baymax.aws.taxi.data.analytics.operators.predict;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import cool.baymax.aws.taxi.data.analytics.pojo.counter.GeoCounter;

/**
 * @author Baymax
 **/
public class DriverCounterPredict extends RichCoFlatMapFunction<GeoCounter, GeoCounter, GeoCounter> {

	private final int timeCurrent;
	private final int timeLast;
	private final int timePredict;
	private ValueState<GeoCounter> currentValueState;
	private ValueState<GeoCounter> lastValueState;

	public DriverCounterPredict(int timeCurrent, int timeLast, int timePredict) {
		this.timeCurrent = timeCurrent;
		this.timeLast = timeLast;
		this.timePredict = timePredict;
	}

	private GeoCounter getPredict(GeoCounter current, GeoCounter last) {
		if (current == null) {
			current = new GeoCounter(last.timestamp, 0, last.geoBits);
		} else if (last == null) {
			last = new GeoCounter(current.timestamp, 0, current.geoBits);
		}
		double predictCount = current.pickupCount / timeCurrent * 0.5 + last.pickupCount / timeLast * 0.5;
		return new GeoCounter(Math.max(current.timestamp, last.timestamp), predictCount * timePredict, current.geoBits);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		currentValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved current", GeoCounter.class));
		lastValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved last", GeoCounter.class));
	}

	@Override
	public void flatMap1(GeoCounter current, Collector<GeoCounter> out) throws Exception {
		out.collect(getPredict(current, lastValueState.value()));
		currentValueState.update(current);
	}

	@Override
	public void flatMap2(GeoCounter last, Collector<GeoCounter> out) throws Exception {
		out.collect(getPredict(currentValueState.value(), last));
		lastValueState.update(last);
	}
}
