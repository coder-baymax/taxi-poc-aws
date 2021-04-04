package cool.baymax.aws.taxi.data.replay.utils;

import java.io.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cool.baymax.aws.taxi.data.replay.pojo.TripRecord;
import cool.baymax.aws.taxi.data.replay.pojo.TripRecordParser;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

/** @author Baymax */
public class TripReader extends Thread {
	static final Logger LOG = LoggerFactory.getLogger(TripReader.class);
	static final Pattern PATTERN = Pattern.compile("^(\\d{4}-\\d{2})/date=(\\d{4}-\\d{2}-\\d{2})");
	final S3Client s3;
	final String bucketName;
	final float speed;
	final int threshold;
	final PriorityBlockingQueue<String> fileQueue;
	final LinkedBlockingQueue<TripRecord> recordQueue;

	private BufferedReader objectStream;
	private TripRecordParser recordParser;

	public TripReader(S3Client s3, String bucketName, String startMonth, float speed) throws InterruptedException {
		this.s3 = s3;
		this.bucketName = bucketName;
		this.speed = speed;
		this.threshold = Math.round(speed * 5);
		this.fileQueue = new PriorityBlockingQueue<>();
		this.recordQueue = new LinkedBlockingQueue<>();

		ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketName).build();
		for (S3Object s3Object : s3.listObjectsV2Paginator(request).contents()) {
			if (s3Object.key().compareTo(startMonth) > 0) {
				Matcher matcher = PATTERN.matcher(s3Object.key());
				if (matcher.find()) {
					String month = matcher.group(1);
					String date = matcher.group(2);
					if (date.startsWith(month)) {
						this.fileQueue.offer(s3Object.key());
					}
				}
			}
		}

		fillRecordQueue();
	}

	private BufferedReader newStream() throws InterruptedException {
		if (fileQueue.isEmpty()) {
			return null;
		} else {
			String key = fileQueue.take();
			GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(key).build();
			InputStream stream = new BufferedInputStream(s3.getObject(request));

			try {
				stream = new CompressorStreamFactory().createCompressorInputStream(stream);
			} catch (CompressorException e) {
				LOG.info("unable to decompress object: {}", e.getMessage());
			}
			return new BufferedReader(new InputStreamReader(stream));
		}
	}

	private void fillRecordQueue() throws InterruptedException {
		while (hasNext() && recordQueue.size() < threshold) {
			String nextLine = null;

			try {
				nextLine = objectStream.readLine();
			} catch (IOException | NullPointerException e) {
				// if the next line cannot be read, that's fine, the next S3 object will be
				// opened and read subsequently
			}

			if (nextLine == null) {
				objectStream = newStream();
				recordParser = new TripRecordParser();
			} else {
				TripRecord record = recordParser.fromString(nextLine);
				if (record != null) {
					recordQueue.offer(record);
				}
			}
		}
	}

	@Override
	public void run() {
		try {
			while (!Thread.currentThread().isInterrupted()) {
				if (!hasNext()) {
					Thread.currentThread().interrupt();
				} else {
					fillRecordQueue();
					Thread.sleep(10);
				}
			}
		} catch (InterruptedException ignored) {
		}
	}

	public int left() {
		return this.recordQueue.size();
	}

	public boolean hasNext() {
		return !this.fileQueue.isEmpty() || !this.recordQueue.isEmpty() || objectStream != null;
	}

	public TripRecord next() throws InterruptedException {
		return hasNext() ? recordQueue.take() : null;
	}
}
