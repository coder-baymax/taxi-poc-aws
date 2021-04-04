package cool.baymax.aws.taxi.data.analytics.utils;

import cool.baymax.aws.taxi.data.analytics.pojo.flink.Location;
import cool.baymax.aws.taxi.data.analytics.pojo.TripRecord;
import cool.baymax.aws.taxi.data.analytics.pojo.counter.LocationCounter;

/**
 * @author Baymax
 **/
public class LocationUtils {
    public static LocationCounter getLocationCounter(TripRecord trip){
        Location location = Location.getClosest(trip.pickUpLat, trip.pickUpLng);
        return new LocationCounter(trip.pickUpTime.toEpochMilli(), 1, location.locationId);
    }
}
