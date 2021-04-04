package cool.baymax.aws.taxi.data.analytics.pojo.counter;

/** @author Baymax */
public class GeoCounter extends TimeCounter {
	public final long geoBits;

	public GeoCounter(long timestamp, double pickupCount, long geoBits) {
		super(timestamp, pickupCount);
		this.geoBits = geoBits;
	}

	@Override
	public String toString() {
		return "GeoCounter{" + "geoHash='" + geoBits + '\'' + ", timestamp=" + timestamp + ", pickupCount="
				+ pickupCount + '}';
	}
}
