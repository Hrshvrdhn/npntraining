package com.npntraining.utils.cron;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class QuartzMain {

	public static void startScheduling() throws SchedulerException {
		try {
			System.out.println("1111");
			JobDetail job = JobBuilder.newJob(QuartzJob.class).build();
			System.out.println("quartz main");
			Trigger t1 = TriggerBuilder.newTrigger().withIdentity("Cron Job 1")
					.withSchedule(CronScheduleBuilder.cronSchedule("0 0/1 * 1/1 * ? *")).build();

			Scheduler sd = StdSchedulerFactory.getDefaultScheduler();
			sd.start();
			sd.scheduleJob(job, t1);
			System.out.println("end--");
		} catch (Exception e) {
			System.out.println("error in Quartz Main");
			e.printStackTrace();
		}
	}
}
