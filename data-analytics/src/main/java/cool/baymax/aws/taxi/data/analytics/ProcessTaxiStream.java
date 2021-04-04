package cool.baymax.aws.taxi.data.analytics;

import java.time.Duration;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Regions;

import cool.baymax.aws.taxi.data.analytics.operators.sinks.AmazonStreamSink;
import cool.baymax.aws.taxi.data.analytics.operators.transformation.DriverStreamTransformation;
import cool.baymax.aws.taxi.data.analytics.operators.transformation.OperatorStreamTransformation;
import cool.baymax.aws.taxi.data.analytics.pojo.TripRecord;
import cool.baymax.aws.taxi.data.analytics.pojo.TripRecordParser;
import cool.baymax.aws.taxi.data.analytics.utils.PrettyTripUtils;

/** @author Baymax */
public class ProcessTaxiStream {
	private static final Logger LOG = LoggerFactory.getLogger(ProcessTaxiStream.class);

	private static final String IN_STREAM_NAME = "taxi-poc-input";
	private static final String OUT_STREAM_NAME = "taxi-poc-trip-record";
	private static final String REDIS_HOST = "taxi-poc-redis.vxe0a7.0001.use1.cache.amazonaws.com";
	private static final int REDIS_PORT = 6379;
	private static final String DEFAULT_REGION_NAME = Regions.getCurrentRegion() == null
			? "eu-west-1"
			: Regions.getCurrentRegion().getName();

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties kinesisConsumerConfig = new Properties();
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, DEFAULT_REGION_NAME);
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "1000");

		DataStream<TripRecord> kinesisStream = env
				.addSource(new FlinkKinesisConsumer<>(IN_STREAM_NAME, new TripRecordParser(), kinesisConsumerConfig));
		DataStream<TripRecord> watermarkedStream = kinesisStream.assignTimestampsAndWatermarks(
				WatermarkStrategy.<TripRecord>forBoundedOutOfOrderness(Duration.ofSeconds(2))
						.withTimestampAssigner((record, timeStamp) -> record.pickUpTime.toEpochMilli()));

		new DriverStreamTransformation(env, REDIS_HOST, REDIS_PORT, watermarkedStream).transformDriverCounter();
		new OperatorStreamTransformation(env, REDIS_HOST, REDIS_PORT, watermarkedStream).transformOperatorCounter();

		FlinkKinesisProducer<String> sink = AmazonStreamSink.createSinkFromConfig(DEFAULT_REGION_NAME, OUT_STREAM_NAME);
		kinesisStream.map(PrettyTripUtils::getPretty).map(record -> String.format("%s\n", record.toString()))
				.addSink(sink);

		env.execute();
	}
}
