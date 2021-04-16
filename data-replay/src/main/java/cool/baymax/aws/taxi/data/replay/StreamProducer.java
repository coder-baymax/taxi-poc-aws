package cool.baymax.aws.taxi.data.replay;

import java.lang.invoke.MethodHandles;

import org.apache.commons.cli.*;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

import cool.baymax.aws.taxi.data.replay.utils.TripBufferedWriter;
import cool.baymax.aws.taxi.data.replay.utils.TripReader;
import redis.clients.jedis.Jedis;
import software.amazon.awssdk.services.s3.S3Client;

/** @author Baymax */
public class StreamProducer {
	private static final Logger LOG = LoggerFactory.getLogger(StreamProducer.class);
	private static final String DEFAULT_REGION_NAME = Regions.getCurrentRegion() == null
			? "eu-west-1"
			: Regions.getCurrentRegion().getName();
	private static final String REDIS_HOST = "taxi-poc-redis.vxe0a7.0001.use1.cache.amazonaws.com";
	private static final int REDIS_PORT = 6379;
	private static final String ES_ENDPOINT = "search-aws-taxi-poc-cntkqynq3uyadf3kmmrl5dnnje.us-east-1.es.amazonaws.com";

	private final KinesisProducer kinesisProducer;
	private final TripReader tripReader;
	private final TripBufferedWriter tripBufferedWriter;

	public StreamProducer(String streamRegion, String streamName, String bucketName, String startMonth, float speed)
			throws InterruptedException {
		KinesisProducerConfiguration producerConfiguration = new KinesisProducerConfiguration().setRegion(streamRegion)
				.setRecordTtl(60_000).setThreadingModel(KinesisProducerConfiguration.ThreadingModel.POOLED)
				.setAggregationEnabled(true);
		this.kinesisProducer = new KinesisProducer(producerConfiguration);

		final S3Client s3 = S3Client.builder().build();
		this.tripReader = new TripReader(s3, bucketName, startMonth, speed);
		this.tripBufferedWriter = new TripBufferedWriter(this.tripReader, kinesisProducer, streamName, 100);
		this.tripReader.start();
		this.tripBufferedWriter.start();
	}

	public static void main(String[] args) throws ParseException, InterruptedException {
		Options options = new Options().addOption("streamRegion", true, "the region of the Kinesis stream")
				.addOption("streamName", true, "the name of the kinesis stream the events are sent to")
				.addOption("bucketName", true, "the bucket containing the raw event data")
				.addOption("startMonth", true, "select the first month of data, format: YYYY-MM")
				.addOption("speed", true, "the speed for replaying data into the kinesis stream");

		CommandLine line = new DefaultParser().parse(options, args);
		if (line.hasOption("help")) {
			new HelpFormatter().printHelp(MethodHandles.lookup().lookupClass().getName(), options);
		} else {
			StreamProducer producer = new StreamProducer(line.getOptionValue("streamRegion", DEFAULT_REGION_NAME),
					line.getOptionValue("streamName", "taxi-poc-input"),
					line.getOptionValue("bucketName", "taxi-poc-formatted"),
					line.getOptionValue("startMonth", "2018-04"),
					Float.parseFloat(line.getOptionValue("speed", "100")));

			producer.produce();
		}
	}

	private void produce() {
		try {
			Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
			System.out.println("PRODUCER START " + " LEFT: " + tripReader.left());
			RestHighLevelClient esClient = new RestHighLevelClient(
					RestClient.builder(new HttpHost(ES_ENDPOINT, 443, "https")));
			long esTimeStamp = 0L;
			Long latestTime = 0L;
			while (latestTime != null) {
				jedis.set("TAXI-POC-LATEST-TIME", String.valueOf(latestTime * 1000));
				latestTime = tripBufferedWriter.fillQueue();
				System.out.println("NEWEST TIME " + latestTime + " LEFT: " + tripReader.left());
				if (latestTime > esTimeStamp + 600) {
					System.out.println("Deleted ES TimeStamp" + (latestTime - 86400 * 7) * 1000);
					esClient.deleteByQueryAsync(
							new DeleteByQueryRequest("trip_record").setQuery(
									new RangeQueryBuilder("pick_up_time").lt((latestTime - 86400 * 7) * 1000)),
							RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {
								@Override
								public void onResponse(BulkByScrollResponse response) {
									long deleted = response.getDeleted();
									System.out.println("Deleted ES records: " + deleted);
								}

								@Override
								public void onFailure(Exception e) {
									System.out.println("!!!Delete ES error!!! " + e.toString());
								}
							});
					esTimeStamp = latestTime;
				}
			}
		} catch (InterruptedException ignored) {
		} finally {
			this.tripReader.interrupt();
			this.tripBufferedWriter.interrupt();

			kinesisProducer.flushSync();
			kinesisProducer.destroy();
		}
	}
}
