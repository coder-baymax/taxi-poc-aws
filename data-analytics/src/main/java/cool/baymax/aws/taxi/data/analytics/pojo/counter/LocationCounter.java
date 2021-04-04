package cool.baymax.aws.taxi.data.analytics.pojo.counter;

/** @author Baymax */
public class LocationCounter extends TimeCounter {
	public final int locationId;

	public LocationCounter(long timestamp, double pickupCount, int locationId) {
		super(timestamp, pickupCount);
		this.locationId = locationId;
	}

	@Override
	public String toString() {
		return "LocationCounter{" + "locationId=" + locationId + '}';
	}
}
