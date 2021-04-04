package cool.baymax.aws.taxi.data.analytics.generator;

import java.time.Instant;
import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import cool.baymax.aws.taxi.data.analytics.pojo.flink.Location;
import cool.baymax.aws.taxi.data.analytics.pojo.TripRecord;

/**
 * @author Baymax
 **/
public class TripRecordGenerator implements SourceFunction<TripRecord> {

	private static final long SLEEP_MILLIS_PER_EVENT = 10;
	private static final long SECONDS_BETWEEN_TRIP = 20;
	private final int locationCount;
	private final int recordLimit;
	private volatile boolean running = true;

	public TripRecordGenerator(int locationCount, int recordLimit) {
		this.locationCount = locationCount;
		this.recordLimit = recordLimit;
	}

	@Override
	public void run(SourceContext<TripRecord> ctx) throws Exception {
		int count = 0;
		int round = 0;
		double singleGap = SECONDS_BETWEEN_TRIP * 1.0 / this.locationCount;
		Random rand = new Random();
		Instant beginTime = Instant.parse("2010-01-01T12:00:00.00Z");
		while (running && count < recordLimit) {
			for (int i = 0; i < this.locationCount; i++) {
				Location pickUp = Location.LOCATIONS[i];
				Location dropOff = Location.LOCATIONS[rand.nextInt(Location.LOCATIONS.length)];
				long timePlus = (long) (SECONDS_BETWEEN_TRIP * round + singleGap * i + rand.nextDouble());

				TripRecord record = new TripRecord();
				record.vendorType = rand.nextInt(2);
				record.passengerCount = rand.nextInt(5);
				record.totalAmount = rand.nextDouble() * 40;
				record.tripDistance = rand.nextDouble() * 20;
				record.pickUpTime = beginTime.plusSeconds(timePlus);
				record.dropOffTime = record.pickUpTime.plusSeconds(rand.nextInt(600));
				record.pickUpLat = pickUp.lat;
				record.pickUpLng = pickUp.lng;
				record.dropOffLat = dropOff.lat;
				record.dropOffLng = dropOff.lng;
				ctx.collectWithTimestamp(record, Instant.now().toEpochMilli());
				count++;
			}
			round++;
			Thread.sleep(TripRecordGenerator.SLEEP_MILLIS_PER_EVENT);
		}
	}

	@Override
	public void cancel() {
		this.running = false;
	}
}
