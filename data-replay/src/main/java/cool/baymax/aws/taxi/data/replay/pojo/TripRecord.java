package cool.baymax.aws.taxi.data.replay.pojo;

import java.time.Instant;

/** @author Baymax */
public class TripRecord {
	public Integer vendorType;
	public Integer passengerCount;
	public Double tripDistance;
	public Double totalAmount;
	public Instant pickUpTime;
	public Instant dropOffTime;
	public Double pickUpLat;
	public Double pickUpLng;
	public Double dropOffLat;
	public Double dropOffLng;

	@Override
	public String toString() {
		return "TripRecord{" + "vendorType=" + vendorType + ", passengerCount=" + passengerCount + ", tripDistance="
				+ tripDistance + ", totalAmount=" + totalAmount + ", pickUpTime=" + pickUpTime + ", dropOffTime="
				+ dropOffTime + ", pickUpLat=" + pickUpLat + ", pickUpLng=" + pickUpLng + ", dropOffLat=" + dropOffLat
				+ ", dropOffLng=" + dropOffLng + '}';
	}
}
