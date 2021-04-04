package cool.baymax.aws.taxi.data.analytics.operators.sinks;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

/**
 * @author Baymax
 **/
public class AmazonStreamSink {

	public static FlinkKinesisProducer<String> createSinkFromConfig(String region, String outStreamName) {
		Properties outputProperties = new Properties();
		outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
		outputProperties.setProperty("AggregationEnabled", "false");

		FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(), outputProperties);
		sink.setDefaultStream(outStreamName);
		sink.setDefaultPartition("0");
		return sink;
	}

}
