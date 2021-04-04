package cool.baymax.aws.taxi.data.analytics.operators.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import cool.baymax.aws.taxi.data.analytics.pojo.counter.LocationCounter;

/**
 * @author Baymax
 **/
public class LocationCounterMapper implements RedisMapper<LocationCounter> {
	private final String keyHeader;

	public LocationCounterMapper(String keyHeader) {
		this.keyHeader = keyHeader;
	}

	@Override
	public RedisCommandDescription getCommandDescription() {
		return new RedisCommandDescription(RedisCommand.SET);
	}

	@Override
	public String getKeyFromData(LocationCounter geoCounter) {
		return String.format("%s&&%d", keyHeader, geoCounter.locationId);
	}

	@Override
	public String getValueFromData(LocationCounter geoCounter) {
		return String.format("%.4f&&%d", geoCounter.pickupCount, geoCounter.timestamp);
	}
}