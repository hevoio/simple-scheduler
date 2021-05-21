 CREATE TABLE `mysql_scheduled_tasks` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `type` varchar(32) NOT NULL,
  `name` varchar(64) NOT NULL,
  `handler_class` varchar(128) NOT NULL,
  `schedule_expression` varchar(32) NOT NULL,
  `parameters` varchar(512) NULL,
  `status` varchar(16) NOT NULL,
  `picked_at` timestamp NULL,
  `execution_time` timestamp NULL,
  `next_execution_time` timestamp NULL,
  `executions` bigint(20) NOT NULL DEFAULT 0,
  `last_failed_at` timestamp NULL,
  `failure_count` INT NOT NULL DEFAULT 0,
  `executor_id` varchar(48) NULL,
  `created_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQ_name` (`name`),
  KEY `IDX_next_execution` (`next_execution_time`)
);