# in-memory-db-scheduler
INSERT

task_name            varchar(100),                  cron-task
task_instance        varchar(100),                  recurring
task_data            blob,                          NULL
execution_time       TIMESTAMP WITH TIME ZONE,      2025-12-16 16:28:39.740236+4:00
picked               BIT,                           0
picked_by            varchar(50),                   NULL
last_success         TIMESTAMP WITH TIME ZONE,      NULL
last_failure         TIMESTAMP WITH TIME ZONE,      NULL
consecutive_failures INT,                           NULL
last_heartbeat       TIMESTAMP WITH TIME ZONE,      NULL
version              BIGINT,                        1
priority             INT,                           NULL

PRIMARY KEY (task_name, task_instance))


