/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;

public class CronMain extends Example {

    public static void main(String[] args) {
        new CronMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        Schedule cron = Schedules.cron("*/1 * * * * *");
        RecurringTask<Void> cronTask =
                Tasks.recurring("cron-task", cron)
                        .execute(
                                (taskInstance, executionContext) -> {
                                    System.out.println(Instant.now().getEpochSecond() + "s 1 -  !" + Thread.currentThread().getName());
                                });


        Schedule cron1 = Schedules.cron("*/1 * * * * *");
        RecurringTask<Void> cronTask1 =
                Tasks.recurring("cron-task-2", cron1)
                        .execute(
                                (taskInstance, executionContext) -> {
                                    System.out.println(Instant.now().getEpochSecond() + "s 2 -  !" + Thread.currentThread().getName());
                                });

        Schedule cron2 = Schedules.cron("*/1 * * * * *");
        RecurringTask<Void> cronTask2 =
                Tasks.recurring("cron-task-3", cron2)
                        .execute(
                                (taskInstance, executionContext) -> {
                                    System.out.println(Instant.now().getEpochSecond() + "s 3 -  !" + Thread.currentThread().getName());
                                });

        if (true) {

            final MapScheduler mapScheduler =
                    MapScheduler.create1(dataSource)
                            .schedulerName(new SchedulerName.Fixed("Scheduler-1"))
                            .startTasks(cronTask, cronTask1, cronTask2)
                            .pollingInterval(Duration.ofSeconds(1))
                            .registerShutdownHook()
                            .build();

            final MapScheduler mapScheduler2 =
                    MapScheduler.create1(dataSource)
                            .schedulerName(new SchedulerName.Fixed("Scheduler-2"))
                            .startTasks(cronTask, cronTask1, cronTask2)
                            .pollingInterval(Duration.ofSeconds(1))
                            .registerShutdownHook()
                            .build();

            mapScheduler.start();
            mapScheduler2.start();
        } else {

            final Scheduler scheduler =
                    Scheduler.create(dataSource)
                            .schedulerName(new SchedulerName.Fixed("Scheduler-1"))
                            .startTasks(cronTask)
                            .pollingInterval(Duration.ofSeconds(1))
                            .registerShutdownHook()
                            .build();
            scheduler.start();
        }





//        final Scheduler scheduler1 =
//                Scheduler.create(dataSource)
//                        .schedulerName(new SchedulerName.Fixed("Scheduler-2"))
//                        .startTasks(cronTask, cronTask1, cronTask2)
//                        .pollingInterval(Duration.ofSeconds(1))
//                        .registerShutdownHook()
//                        .build();
//
//        final Scheduler scheduler2 =
//                Scheduler.create(dataSource)
//                        .schedulerName(new SchedulerName.Fixed("Scheduler-3"))
//                        .startTasks(cronTask, cronTask1, cronTask2)
//                        .pollingInterval(Duration.ofSeconds(1))
//                        .registerShutdownHook()
//                        .build();




//        scheduler1.start();
//        scheduler2.start();
    }
}