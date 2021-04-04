package cool.baymax.aws.taxi.data.analytics.operators.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import cool.baymax.aws.taxi.data.analytics.operators.mapper.LocationCounterMapper;
import cool.baymax.aws.taxi.data.analytics.operators.predict.OperatorCounterPredict;
import cool.baymax.aws.taxi.data.analytics.pojo.TripRecord;
import cool.baymax.aws.taxi.data.analytics.pojo.counter.LocationCounter;
import cool.baymax.aws.taxi.data.analytics.utils.GeoUtils;
import cool.baymax.aws.taxi.data.analytics.utils.LocationUtils;

/**
 * @author Baymax
 **/
public class OperatorStreamTransformation {
	public static final int OPERATOR_CURRENT_TIME = 10;
	public static final int OPERATOR_LAST_TIME = 30;
	public static final int OPERATOR_PREDICT_TIME = 10;

	private final StreamExecutionEnvironment env;
	private final String redisHost;
	private final int redisPort;
	private final DataStream<TripRecord> kinesisStream;

	public OperatorStreamTransformation(StreamExecutionEnvironment env, String redisHost, int redisPort,
			DataStream<TripRecord> kinesisStream) {
		this.env = env;
		this.redisHost = redisHost;
		this.redisPort = redisPort;
		this.kinesisStream = kinesisStream;
	}

	private DataStream<LocationCounter> getLocationCounter(DataStream<LocationCounter> stream, Time size, Time slide) {
		return stream.keyBy(counter -> counter.locationId).timeWindow(size, slide)
				.reduce((r1, r2) -> new LocationCounter(Math.max(r1.timestamp, r2.timestamp),
						r1.pickupCount + r2.pickupCount, r1.locationId));
	}

	public void transformOperatorCounter() {
		DataStream<LocationCounter> stream = kinesisStream.filter(GeoUtils::hasValidCoordinates)
				.map(LocationUtils::getLocationCounter);

		DataStream<LocationCounter> operatorCurrentCounter = getLocationCounter(stream,
				Time.minutes(OPERATOR_CURRENT_TIME), Time.seconds(OPERATOR_CURRENT_TIME * 3));
		DataStream<LocationCounter> operatorLastCounter = getLocationCounter(stream, Time.minutes(OPERATOR_LAST_TIME),
				Time.seconds(OPERATOR_LAST_TIME * 3));
		DataStream<LocationCounter> operatorPredictCounter = operatorCurrentCounter.keyBy(counter -> counter.locationId)
				.connect(operatorLastCounter.keyBy(counter -> counter.locationId))
				.flatMap(new OperatorCounterPredict(OPERATOR_CURRENT_TIME, OPERATOR_LAST_TIME, OPERATOR_PREDICT_TIME));

		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost(redisHost).setPort(redisPort).build();
		operatorCurrentCounter.addSink(new RedisSink<>(conf, new LocationCounterMapper("OCC")));
		operatorLastCounter.addSink(new RedisSink<>(conf, new LocationCounterMapper("OLC")));
		operatorPredictCounter.addSink(new RedisSink<>(conf, new LocationCounterMapper("OPC")));
	}

}
