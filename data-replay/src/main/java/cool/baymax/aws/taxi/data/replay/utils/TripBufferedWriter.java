package cool.baymax.aws.taxi.data.replay.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.util.concurrent.ListenableFuture;

import cool.baymax.aws.taxi.data.replay.pojo.TripRecord;
import software.amazon.awssdk.services.s3.S3Client;

/** @author Baymax */
public class TripBufferedWriter extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(TripBufferedWriter.class);
	private static final int MILLI_SECOND = 1000;

	final String streamName;
	final KinesisProducer kinesisProducer;
	final S3Client s3;
	final String bucketName;
	final float speed;
	final int timeInterval;
	final TripReader tripReader;
	final Semaphore semaphore = new Semaphore(0);
	final int singleRoundCount;
	private final AtomicReference<TripRecord> lastRecord = new AtomicReference<>();
	private volatile boolean finished = false;
	private int timeRecord = 0;
	private float speedRecord = 0;

	public TripBufferedWriter(TripReader tripReader, KinesisProducer kinesisProducer, String streamName,
			int timeInterval) {
		this.tripReader = tripReader;
		this.kinesisProducer = kinesisProducer;
		this.streamName = streamName;
		this.s3 = tripReader.s3;
		this.bucketName = tripReader.bucketName;
		this.speed = tripReader.speed;
		this.singleRoundCount = (int) ((speed * timeInterval - 1) / MILLI_SECOND + 1);
		this.timeInterval = timeInterval;
	}

	@Override
	public void run() {
		ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();

		try {
			while (!Thread.currentThread().isInterrupted()) {
				semaphore.acquire();
				if (tripReader.hasNext()) {
					TripRecord record = tripReader.next();
					lastRecord.set(record);
					byte[] bytes = objectMapper.writeValueAsString(record).getBytes(StandardCharsets.UTF_8);
					ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
					ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(streamName,
							Integer.toString(record.hashCode()), byteBuffer);
					LOG.info("sent event {}", record);
				} else {
					finished = true;
					Thread.currentThread().interrupt();
				}
			}
		} catch (InterruptedException | JsonProcessingException ignored) {
		}
	}

	public Long fillQueue() throws InterruptedException {
		speedRecord += speed;
		while (timeRecord < MILLI_SECOND && !finished) {
			int roundCount = singleRoundCount > speedRecord ? (int) speedRecord : singleRoundCount;
			semaphore.release(roundCount);
			speedRecord -= roundCount;
			Thread.sleep(timeInterval);
			timeRecord += timeInterval;
		}
		timeRecord -= MILLI_SECOND;
		TripRecord record = lastRecord.get();
		return finished ? null : record == null ? 0 : record.pickUpTime.getEpochSecond();
	}
}
