def loads_buckets_result(result, need_count=False):
    if 'buckets' not in result and 'terms' not in result:
        new_result = {}
        for key, value in result.items():
            if 'buckets' in value:
                new_result[key] = loads_buckets_result(result[key], need_count)
            elif 'value' in value:
                new_result[key] = value
        return new_result
    else:
        result = result['buckets'] if 'buckets' in result else result['terms']['buckets']
        new_result = []
        for item in result:
            single = {}
            for key, value in item.items():
                if not need_count and key == 'doc_count':
                    continue
                if isinstance(value, dict):
                    if 'value' in value:
                        single[key] = value['value']
                    elif 'terms' in value or 'buckets' in value:
                        single[key] = loads_buckets_result(value, need_count)
                if key not in single:
                    single[key] = value
            new_result.append(single)
        return [x['key'] for x in new_result] if all(len(x) <= 1 for x in new_result) else new_result


class EsAggBuilder:

    def __init__(self, search_or_bucket, root_builder=None):
        if root_builder is not None:
            self.search = None
            self.aggs = search_or_bucket
            self.root_builder = root_builder
        else:
            self.search = search_or_bucket.extra(size=0)
            self.aggs = self.search.aggs
            self.root_builder = None

    def bucket_terms(self, field, bucket_name="terms", size=0xFFFFFFF, **kwargs):
        return self.__class__(self.aggs.bucket(
            bucket_name, "terms", field=field, size=size, **kwargs
        ), self.root_builder or self)

    def bucket_calendar_histogram(self, period, field="ctime", bucket_name="timeline", **kwargs):
        return self.__class__(self.aggs.bucket(
            bucket_name, "date_histogram", field=field, calendar_interval=period, **kwargs
        ), self.root_builder or self)

    def bucket_fixed_histogram(self, period, field="ctime", bucket_name="timeline", **kwargs):
        return self.__class__(self.aggs.bucket(
            bucket_name, "date_histogram", field=field, fixed_interval=period, **kwargs
        ), self.root_builder or self)

    def bucket_histogram(self, period, field="money", bucket_name="histogram", **kwargs):
        return self.__class__(self.aggs.bucket(
            bucket_name, "histogram", field=field, interval=period, **kwargs
        ), self.root_builder or self)

    def metric_terms(self, field, metric_name="terms_values", size=0x7FFFFFFF, **kwargs):
        self.aggs.metric(metric_name, "terms", field=field, size=size, **kwargs)
        return self

    def metric_max(self, field, metric_name="max_values", **kwargs):
        self.aggs.metric(metric_name, "max", field=field, **kwargs)
        return self

    def metric_min(self, field, metric_mane="min_values", **kwargs):
        self.aggs.metric(metric_mane, "min", field=field, **kwargs)
        return self

    def metric_avg(self, field, metric_name="avg", **kwargs):
        self.aggs.metric(metric_name, 'avg', field=field, **kwargs)
        return self

    def metric_sum(self, field, metric_name="sum", **kwargs):
        self.aggs.metric(metric_name, 'sum', field=field, **kwargs)
        return self

    def get_search(self):
        if self.search is None:
            if self.root_builder is not None:
                return self.root_builder.get_search()
            else:
                raise Exception("error, this is bucket")
        else:
            return self.search

    def execute(self):
        print(self.get_search().to_dict())
        return self.get_search().execute()

    def extract_result(self, need_count=False):
        result = self.execute().to_dict()
        return [] if 'aggregations' not in result else \
            loads_buckets_result(result.get('aggregations', {}), need_count)
