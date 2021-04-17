package cool.baymax.aws.taxi.data.analytics.pojo.flink;

import java.time.Instant;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author Baymax
 **/
public class PrettyTrip {
	private static final Gson GSON = new GsonBuilder()
			.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

	public Integer vendorType;
	public Integer passengerCount;
	public Double tripDistance;
	public Double totalAmount;
	public Long pickUpTime;
	public Long dropOffTime;
	public long duration;
	public Double speed;
	public Integer pickUpLocationId;
	public String pickUpLocationBorough;
	public String pickUpLocation;
	public Integer dropOffLocationId;
	public String dropOffLocationBorough;
	public String dropOffLocation;

	public static void main(String[] args) {
		PrettyTrip pretty = new PrettyTrip();
		pretty.vendorType = 0;
		pretty.passengerCount = 1;
		pretty.tripDistance = 1.1;
		pretty.totalAmount = 1.2;
		pretty.pickUpTime = Instant.now().toEpochMilli();
		pretty.dropOffTime = Instant.now().toEpochMilli();
		pretty.duration = 10L;
		pretty.speed = 10.2;
		pretty.pickUpLocationId = 20;
		pretty.pickUpLocationBorough = "borough";
		pretty.pickUpLocation = "40.8972233, -73.8860668";
		pretty.dropOffLocationId = 30;
		pretty.dropOffLocationBorough = "borough";
		pretty.dropOffLocation = "40.8972233, -73.8860668";

		System.out.println(pretty.toString());
	}

	@Override
	public String toString() {
		return GSON.toJson(this);
	}
}
