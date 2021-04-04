package cool.baymax.aws.taxi.data.analytics.operators.predict;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import cool.baymax.aws.taxi.data.analytics.pojo.counter.LocationCounter;

/**
 * @author Baymax
 **/
public class OperatorCounterPredict extends RichCoFlatMapFunction<LocationCounter, LocationCounter, LocationCounter> {

	protected final int timeCurrent;
	protected final int timeLast;
	protected final int timePredict;
	protected ValueState<LocationCounter> currentValueState;
	protected ValueState<LocationCounter> lastValueState;

	public OperatorCounterPredict(int timeCurrent, int timeLast, int timePredict) {
        this.timeCurrent = timeCurrent;
        this.timeLast = timeLast;
        this.timePredict = timePredict;
    }

	private LocationCounter getPredict(LocationCounter current, LocationCounter last) {
		if (current == null) {
			current = new LocationCounter(last.timestamp, 0, last.locationId);
		} else if (last == null) {
			last = new LocationCounter(current.timestamp, 0, current.locationId);
		}
		double predictCount = current.pickupCount / timeCurrent * 0.5 + last.pickupCount / timeLast * 0.5;
		return new LocationCounter(Math.max(current.timestamp, last.timestamp), predictCount * timePredict,
				current.locationId);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		currentValueState = getRuntimeContext()
				.getState(new ValueStateDescriptor<>("saved current", LocationCounter.class));
		lastValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved last", LocationCounter.class));
	}

	@Override
	public void flatMap1(LocationCounter current, Collector<LocationCounter> out) throws Exception {
		out.collect(getPredict(current, lastValueState.value()));
		currentValueState.update(current);
	}

	@Override
	public void flatMap2(LocationCounter last, Collector<LocationCounter> out) throws Exception {
		out.collect(getPredict(currentValueState.value(), last));
		lastValueState.update(last);
	}
}
