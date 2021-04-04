package cool.baymax.aws.taxi.data.analytics.pojo.counter;

/** @author Baymax */
public abstract class TimeCounter {
	public final long timestamp;
	public final double pickupCount;

	public TimeCounter(long timestamp, double pickupCount) {
		this.timestamp = timestamp;
		this.pickupCount = pickupCount;
	}
}
