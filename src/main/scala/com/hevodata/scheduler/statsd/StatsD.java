package com.hevodata.scheduler.statsd;

import com.timgroup.statsd.StatsDClient;

import java.util.List;

/**
 StatsD interface
 */
abstract class StatsD {

    static void incr(StatsDClient statsDClient, String aspect, List<String> tags) {
        statsDClient.incrementCounter(aspect, getDataDogTags(tags));
    }

    static void count(StatsDClient statsDClient, String aspect, long delta, List<String> tags) {
        statsDClient.count(aspect, delta, getDataDogTags(tags));
    }

    static void gauge(StatsDClient statsDClient, String aspect, long value, List<String> tags) {
        statsDClient.recordGaugeValue(aspect, value, getDataDogTags(tags));
    }

    static void time(StatsDClient statsDClient, String aspect, long value, List<String> tags) {
        statsDClient.recordExecutionTime(aspect, value, getDataDogTags(tags));
    }

    private static String[] getDataDogTags(List<String> tags) {
        return tags.toArray(new String[tags.size()]);
    }
}
