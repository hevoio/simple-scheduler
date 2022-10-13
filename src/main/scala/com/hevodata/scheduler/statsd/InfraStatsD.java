package com.hevodata.scheduler.statsd;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import java.util.List;

/**
 * Infra client for statsD.
 * It talks to statsD client on port 8126, similar to services port.
 */
public class InfraStatsD extends StatsD {

    public static class Aspect {

        private Aspect() {}

        public static final String TASKS_ADDED = "greenseer_add_tasks";
        public static final String TRIGGER_RUN = "greenseer_trigger_run";
        public static final String TASKS_DELETED = "greenseer_delete_tasks";
        public static final String TASKS_DELAY = "greenseer_task_delay";
        public static final String TASKS_RUNNING = "greenseer_task_running";
        public static final String TASKS_GLOBAL_LOCK_ACQUIRE = "greenseer_global_lock_acquire";
        public static final String TASKS_GLOBAL_LOCK_ACQUIRE_FAILED = "greenseer_global_lock_acquire_failed";
        public static final String TASKS_GLOBAL_LOCK_RETRIES = "greenseer_global_lock_retries";
        public static final String TASKS_FETCHED = "greenseer_tasks_fetched";
        public static final String TASKS_FAILED = "greenseer_tasks_failed";
        public static final String TASKS_SUCCESS = "greenseer_tasks_success";
        public static final String TASKS_MISSING = "greenseer_tasks_missing_handler";
        public static final String TASKS_CLEANED = "greenseer_tasks_cleaned";
    }

    private static final String METRICS_PREFIX = "";
    private static final StatsDClient statsD = new NonBlockingStatsDClient(METRICS_PREFIX, "localhost", 8126);

    public static void incr(String aspect, List<String> tags) {
        StatsD.incr(statsD, aspect, tags);
    }

    public static void count(String aspect, long delta, List<String> tags) {
        StatsD.count(statsD, aspect, delta, tags);
    }

    public static void gauge(String aspect, long value, List<String> tags) {
        StatsD.gauge(statsD, aspect, value, tags);
    }

    public static void time(String aspect, long value, List<String> tags) {
        StatsD.time(statsD, aspect, value, tags);
    }
}
