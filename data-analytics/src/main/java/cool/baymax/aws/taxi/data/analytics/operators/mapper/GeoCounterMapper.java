package cool.baymax.aws.taxi.data.analytics.operators.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import cool.baymax.aws.taxi.data.analytics.pojo.counter.GeoCounter;

/**
 * @author Baymax
 **/
public class GeoCounterMapper implements RedisMapper<GeoCounter> {
	private final String keyHeader;
	private final int hashLen;

	public GeoCounterMapper(String keyHeader, int hashLen) {
		this.keyHeader = keyHeader;
		this.hashLen = hashLen;
	}

	@Override
	public RedisCommandDescription getCommandDescription() {
		return new RedisCommandDescription(RedisCommand.SET);
	}

	@Override
	public String getKeyFromData(GeoCounter geoCounter) {
		return String.format("%s&&%s", keyHeader, Long.toHexString(geoCounter.geoBits >> (64 - hashLen)));
	}

	@Override
	public String getValueFromData(GeoCounter geoCounter) {
		return String.format("%.4f&&%d", geoCounter.pickupCount, geoCounter.timestamp);
	}
}
