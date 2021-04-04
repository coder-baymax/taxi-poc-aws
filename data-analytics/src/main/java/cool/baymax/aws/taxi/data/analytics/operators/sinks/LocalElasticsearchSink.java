package cool.baymax.aws.taxi.data.analytics.operators.sinks;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Baymax
 **/
public class LocalElasticsearchSink {

	private static final int FLUSH_MAX_ACTIONS = 10_000;
	private static final long FLUSH_INTERVAL_MILLIS = 1_000;
	private static final int FLUSH_MAX_SIZE_MB = 1;

	private static final Logger LOG = LoggerFactory.getLogger(LocalElasticsearchSink.class);

	public static <PrettyTrip> ElasticsearchSink<PrettyTrip> buildElasticsearchSink(String elasticsearchEndpoint,
			String indexName, String type) {
		final List<HttpHost> httpHosts = Collections.singletonList(HttpHost.create(elasticsearchEndpoint));

		ElasticsearchSink.Builder<PrettyTrip> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
				new ElasticsearchSinkFunction<PrettyTrip>() {
					public IndexRequest createIndexRequest(PrettyTrip element) {
						return Requests.indexRequest().index(indexName).type(type).source(element.toString(),
								XContentType.JSON);
					}

					@Override
					public void process(PrettyTrip element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				});

		esSinkBuilder.setBulkFlushMaxActions(FLUSH_MAX_ACTIONS);
		esSinkBuilder.setBulkFlushInterval(FLUSH_INTERVAL_MILLIS);
		esSinkBuilder.setBulkFlushMaxSizeMb(FLUSH_MAX_SIZE_MB);
		esSinkBuilder.setBulkFlushBackoff(true);

		esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

		return esSinkBuilder.build();
	}

}
