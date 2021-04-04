package cool.baymax.aws.taxi.data.analytics.operators.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import ch.hsr.geohash.GeoHash;
import cool.baymax.aws.taxi.data.analytics.operators.mapper.GeoCounterMapper;
import cool.baymax.aws.taxi.data.analytics.operators.mapper.GeoLocationMapper;
import cool.baymax.aws.taxi.data.analytics.operators.predict.DriverCounterPredict;
import cool.baymax.aws.taxi.data.analytics.pojo.TripRecord;
import cool.baymax.aws.taxi.data.analytics.pojo.counter.GeoCounter;
import cool.baymax.aws.taxi.data.analytics.utils.GeoUtils;

/**
 * @author Baymax
 **/
public class DriverStreamTransformation {
	public static final int DRIVER_CURRENT_TIME = 5;
	public static final int DRIVER_LAST_TIME = 30;
	public static final int DRIVER_PREDICT_TIME = 5;

	public static final String DRIVER_STORED_KEY = "driver_locations";
	public static final int DRIVER_HASH_LEN = 32;

	private final StreamExecutionEnvironment env;
	private final String redisHost;
	private final int redisPort;
	private final DataStream<TripRecord> kinesisStream;

	public DriverStreamTransformation(StreamExecutionEnvironment env, String redisHost, int redisPort,
			DataStream<TripRecord> kinesisStream) {
		this.env = env;
		this.redisHost = redisHost;
		this.redisPort = redisPort;
		this.kinesisStream = kinesisStream;
	}

	private DataStream<GeoCounter> getGeoCounter(DataStream<GeoCounter> stream, Time size, Time slide) {
		return stream.keyBy(counter -> counter.geoBits).timeWindow(size, slide)
				.reduce((r1, r2) -> new GeoCounter(Math.max(r1.timestamp, r2.timestamp),
						r1.pickupCount + r2.pickupCount, r1.geoBits));
	}

	public void transformDriverCounter() {
		DataStream<GeoCounter> stream = kinesisStream.filter(GeoUtils::hasValidCoordinates)
				.map(record -> new GeoCounter(record.pickUpTime.toEpochMilli(), 1,
						GeoHash.withBitPrecision(record.pickUpLat, record.pickUpLng, DRIVER_HASH_LEN).longValue()));

		DataStream<GeoCounter> driverCurrentCounter = getGeoCounter(stream, Time.minutes(DRIVER_CURRENT_TIME),
				Time.seconds(DRIVER_CURRENT_TIME * 3));
		DataStream<GeoCounter> driverLastCounter = getGeoCounter(stream, Time.minutes(DRIVER_LAST_TIME),
				Time.seconds(DRIVER_LAST_TIME * 3));
		DataStream<GeoCounter> driverPredictCounter = driverCurrentCounter.keyBy(counter -> counter.geoBits)
				.connect(driverLastCounter.keyBy(counter -> counter.geoBits))
				.flatMap(new DriverCounterPredict(DRIVER_CURRENT_TIME, DRIVER_LAST_TIME, DRIVER_PREDICT_TIME));

		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost(redisHost).setPort(redisPort).build();
		driverCurrentCounter.addSink(new RedisSink<>(conf, new GeoLocationMapper(DRIVER_STORED_KEY, DRIVER_HASH_LEN)));
		driverCurrentCounter.addSink(new RedisSink<>(conf, new GeoCounterMapper("DCC", DRIVER_HASH_LEN)));
		driverLastCounter.addSink(new RedisSink<>(conf, new GeoCounterMapper("DLC", DRIVER_HASH_LEN)));
		driverPredictCounter.addSink(new RedisSink<>(conf, new GeoCounterMapper("DPC", DRIVER_HASH_LEN)));
	}

}
