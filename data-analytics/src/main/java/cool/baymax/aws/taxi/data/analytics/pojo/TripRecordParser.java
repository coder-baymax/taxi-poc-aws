package cool.baymax.aws.taxi.data.analytics.pojo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

/** @author Baymax */
public class TripRecordParser extends AbstractDeserializationSchema<TripRecord> {
	private static final Logger LOG = LoggerFactory.getLogger(TripRecordParser.class);

	public static void main(String[] args) throws IOException {
		ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();

		TripRecord record = new TripRecord();
		record.passengerCount = 10;
		record.dropOffTime = Instant.now();
		System.out.println(record);
		byte[] bytes = objectMapper.writeValueAsString(record).getBytes(StandardCharsets.UTF_8);

		TripRecordParser parser = new TripRecordParser();
		TripRecord newRecord = parser.deserialize(bytes);
		System.out.println(newRecord);
	}

	@Override
	public TripRecord deserialize(byte[] bytes) throws IOException {
		ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
		return objectMapper.readValue(bytes, TripRecord.class);
	}

	@Override
	public boolean isEndOfStream(TripRecord event) {
		return false;
	}

	@Override
	public TypeInformation<TripRecord> getProducedType() {
		return TypeExtractor.getForClass(TripRecord.class);
	}
}
