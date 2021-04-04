package cool.baymax.aws.taxi.data.analytics;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;

import cool.baymax.aws.taxi.data.analytics.generator.TripRecordGenerator;
import cool.baymax.aws.taxi.data.analytics.operators.sinks.LocalElasticsearchSink;
import cool.baymax.aws.taxi.data.analytics.operators.transformation.DriverStreamTransformation;
import cool.baymax.aws.taxi.data.analytics.operators.transformation.OperatorStreamTransformation;
import cool.baymax.aws.taxi.data.analytics.pojo.TripRecord;
import cool.baymax.aws.taxi.data.analytics.pojo.flink.PrettyTrip;
import cool.baymax.aws.taxi.data.analytics.utils.PrettyTripUtils;

/**
 * @author Baymax
 **/
public class ProcessTaxiStreamTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		String redisHost = "localhost";
		int redisPort = 63790;
		String esEndPoint = "localhost:19200";

		DataStream<TripRecord> kinesisStream = env.addSource(new TripRecordGenerator(200, 30000));
		DataStream<TripRecord> watermarkedStream = kinesisStream.assignTimestampsAndWatermarks(
				WatermarkStrategy.<TripRecord>forBoundedOutOfOrderness(Duration.ofSeconds(2))
						.withTimestampAssigner((record, timeStamp) -> record.pickUpTime.toEpochMilli()));

//		new DriverStreamTransformation(env, redisHost, redisPort, watermarkedStream).transformDriverCounter();
//		new OperatorStreamTransformation(env, redisHost, redisPort, watermarkedStream).transformOperatorCounter();

		ElasticsearchSink<PrettyTrip> sink = LocalElasticsearchSink.buildElasticsearchSink(esEndPoint, "trip_record",
				"trip_record");
		kinesisStream.map(PrettyTripUtils::getPretty).addSink(sink);

		env.execute();
	}
}
