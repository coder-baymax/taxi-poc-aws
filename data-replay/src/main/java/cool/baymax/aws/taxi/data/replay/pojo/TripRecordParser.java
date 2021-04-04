package cool.baymax.aws.taxi.data.replay.pojo;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

/** @author Baymax */
public class TripRecordParser {

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-ddHH:mm:ss")
			.withZone(ZoneId.of("America/New_York"));
	private String[] header;

	public TripRecordParser() {
		this.header = null;
	}

	public static void main(String[] args) throws IOException {
		String[] strings = {
				"passenger_count,trip_distance,pickup_datetime,pickup_longitude,dropoff_longitude,vendor_type,pickup_latitude,dropoff_latitude,dropoff_datetime,total_amount",
				"2,0.90000000000000002,2009-01-01 00:00:00,-73.997484,-74.005938,1,40.725954000000002,40.735702000000003,2009-01-01 00:05:03,5.4000000000000004",
				"1,1.3,2009-01-01 00:00:00,-73.965918000000002,-73.949610000000007,0,40.771242999999998,40.777056999999999,2009-01-01 00:04:12,5.7999999999999998",
				"1,1,2009-01-01 00:00:02,-73.964796000000007,-73.977750999999998,0,40.767392999999998,40.773746000000003,2009-01-01 00:05:40,\"\"",};

		ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
		TripRecordParser parser = new TripRecordParser();
		for (String line : strings) {
			TripRecord record = parser.fromString(line);
			if (record == null) {
				continue;
			}
			String s = objectMapper.writeValueAsString(record);
			TripRecord newRecord = objectMapper.readValue(s, TripRecord.class);
			System.out.println("--------------------------------------");
			System.out.println(record);
			System.out.println(s);
			System.out.println(newRecord.toString());
		}
	}

	private static Instant parseTime(String s) {
		try {
			return Instant.from(FORMATTER.parse(s));
		} catch (DateTimeParseException e) {
			System.out.println("parse time error: " + s);
			return null;
		}
	}

	private static Integer parseInt(String s) {
		try {
			return Integer.parseInt(s);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static Double parseDouble(String s) {
		try {
			return Double.parseDouble(s);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	public TripRecord fromString(String string) {
		if (this.header == null) {
			this.header = string.split(",");
			return null;
		} else {
			TripRecord record = new TripRecord();

			String[] info = string.split(",");
			for (int j = 0; j < info.length; j++) {
				String single = info[j].replaceAll("[\"\\s]", "");
				if (single.isEmpty()) {
					continue;
				}
				if ("passenger_count".equals(header[j])) {
					record.passengerCount = parseInt(single);
				} else if ("trip_distance".equals(header[j])) {
					record.tripDistance = parseDouble(single);
				} else if ("total_amount".equals(header[j])) {
					record.totalAmount = parseDouble(single);
				} else if ("pickup_datetime".equals(header[j])) {
					record.pickUpTime = parseTime(single);
				} else if ("dropoff_datetime".equals(header[j])) {
					record.dropOffTime = parseTime(single);
				} else if ("pickup_longitude".equals(header[j])) {
					record.pickUpLng = parseDouble(single);
				} else if ("dropoff_longitude".equals(header[j])) {
					record.dropOffLng = parseDouble(single);
				} else if ("pickup_latitude".equals(header[j])) {
					record.pickUpLat = parseDouble(single);
				} else if ("dropoff_latitude".equals(header[j])) {
					record.dropOffLat = parseDouble(single);
				} else if ("vendor_type".equals(header[j])) {
					record.vendorType = parseInt(single);
				}
			}

			return record;
		}
	}
}
