package cool.baymax.aws.taxi.data.analytics.utils;

import cool.baymax.aws.taxi.data.analytics.pojo.TripRecord;
import cool.baymax.aws.taxi.data.analytics.pojo.flink.Location;
import cool.baymax.aws.taxi.data.analytics.pojo.flink.PrettyTrip;

/**
 * @author Baymax
 **/
public class PrettyTripUtils {
	public static PrettyTrip getPretty(TripRecord record) {
		PrettyTrip pretty = new PrettyTrip();

		pretty.vendorType = record.vendorType == null ? 0 : record.vendorType;
		pretty.passengerCount = record.passengerCount;
		pretty.tripDistance = record.tripDistance;
		pretty.totalAmount = record.totalAmount;
		pretty.pickUpTime = record.pickUpTime == null ? null : record.pickUpTime.toEpochMilli();
		pretty.dropOffTime = record.dropOffTime == null ? null : record.dropOffTime.toEpochMilli();

		if (record.pickUpTime != null && record.dropOffTime != null) {
			pretty.duration = record.dropOffTime.getEpochSecond() - record.pickUpTime.getEpochSecond();
		}
		if (GeoUtils.validCoordinates(record.pickUpLat, record.pickUpLng)) {
			Location location = Location.getClosest(record.pickUpLat, record.pickUpLng);
			pretty.pickUpLocationId = location.locationId;
			pretty.pickUpLocationBorough = location.borough;
			pretty.pickUpLocation = String.format("%.10f,%.10f", location.lat, location.lng);
		}
		if (GeoUtils.validCoordinates(record.dropOffLat, record.dropOffLng)) {
			Location location = Location.getClosest(record.dropOffLat, record.dropOffLng);
			pretty.dropOffLocationId = location.locationId;
			pretty.dropOffLocationBorough = location.borough;
			pretty.dropOffLocation = String.format("%.10f,%.10f", location.lat, location.lng);
		}

		return pretty;
	}

}
