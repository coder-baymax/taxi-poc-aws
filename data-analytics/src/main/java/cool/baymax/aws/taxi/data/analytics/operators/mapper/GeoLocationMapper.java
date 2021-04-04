package cool.baymax.aws.taxi.data.analytics.operators.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import cool.baymax.aws.taxi.data.analytics.pojo.counter.GeoCounter;

/**
 * @author Baymax
 **/
public class GeoLocationMapper implements RedisMapper<GeoCounter> {
	private final String storedKey;
	private final int hashLen;

	public GeoLocationMapper(String storedKey, int hashLen) {
		this.storedKey = storedKey;
		this.hashLen = hashLen;
	}

	@Override
	public RedisCommandDescription getCommandDescription() {
		return new RedisCommandDescription(RedisCommand.ZADD, storedKey);
	}

	@Override
	public String getKeyFromData(GeoCounter geoCounter) {
		return Long.toHexString(geoCounter.geoBits >> (64 - hashLen));
	}

	@Override
	public String getValueFromData(GeoCounter geoCounter) {
		return String.valueOf(geoCounter.geoBits >> (64 - hashLen));
	}
}
