package net.unit8.job_streamer.control_bus;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.quartz.DailyTimeIntervalScheduleBuilder;
import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TimeOfDay;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.OperableTrigger;

/**
 * @kawasima
 */
public class HolidayTest {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    private List<Date> fireJobWithCalendar(Date startAt, HolidayAndWeeklyCalendar calendar) throws ParseException, SchedulerException {
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        Scheduler scheduler = schedulerFactory.getScheduler();
        scheduler.clear();
        scheduler.addCalendar("holidays", calendar, false, false);

        JobDetail jobDetail = JobBuilder
                .newJob(ExampleJob.class)
                .withIdentity("job1", "group1")
                .build();

        DailyTimeIntervalTrigger trigger = TriggerBuilder
                .newTrigger()
                .withSchedule(
                        DailyTimeIntervalScheduleBuilder
                                .dailyTimeIntervalSchedule()
                                .withIntervalInHours(24)
                                .startingDailyAt(TimeOfDay.hourAndMinuteOfDay(0, 0))
                                .onEveryDay())
                .startAt(startAt)
                .modifiedByCalendar("holidays")
                .build();
        scheduler.scheduleJob(jobDetail, trigger);

        return TriggerUtils.computeFireTimes((OperableTrigger) trigger, calendar, 5);
    }

    @Test
    public void neverFire() throws ParseException, SchedulerException {
        HolidayAndWeeklyCalendar holidayCalendar = new HolidayAndWeeklyCalendar();
        holidayCalendar.addExcludedDate(sdf.parse("20150429"));
        holidayCalendar.addExcludedDate(sdf.parse("20150503"));
        holidayCalendar.addExcludedDate(sdf.parse("20150504"));
        holidayCalendar.addExcludedDate(sdf.parse("20150505"));
        holidayCalendar.setDaysExcluded(new boolean[]{true, true, true, true, true, true, true});

        try {
            fireJobWithCalendar(sdf.parse("20150428"), holidayCalendar);
            Assert.fail("Throw SchedulerException");
        } catch (SchedulerException e) {

        }
    }
    
    @Test
    public void fireWeekday() throws ParseException, SchedulerException {
        HolidayAndWeeklyCalendar holidayCalendar = new HolidayAndWeeklyCalendar();

        List<Date> fireDays = fireJobWithCalendar(sdf.parse("20150715"), holidayCalendar);
        Assert.assertArrayEquals(new Date[]{sdf.parse("20150715"), sdf.parse("20150716"), sdf.parse("20150717"), sdf.parse("20150720"), sdf.parse("20150721")}, fireDays.toArray());
    }
    
    @Test
    public void fireWeekdayWithDayStart() throws ParseException, SchedulerException {
        HolidayAndWeeklyCalendar holidayCalendar = new HolidayAndWeeklyCalendar();
        // Bussiness date start at 2 O'clock.      
        holidayCalendar.setDayStart(7200000L);
        
        List<Date> fireDays = fireJobWithCalendar(sdf.parse("20150715"), holidayCalendar);
        Assert.assertArrayEquals(new Date[]{sdf.parse("20150715"), sdf.parse("20150716"), sdf.parse("20150717"), sdf.parse("20150718"), sdf.parse("20150721")}, fireDays.toArray());
    }

    @Test
    public void addHoliday() throws ParseException, SchedulerException {
        HolidayAndWeeklyCalendar holidayCalendar = new HolidayAndWeeklyCalendar();

        holidayCalendar.addExcludedDate(sdf.parse("20150720"));
        List<Date> fireDays = fireJobWithCalendar(sdf.parse("20150715"), holidayCalendar);
        Assert.assertArrayEquals(new Date[]{sdf.parse("20150715"), sdf.parse("20150716"), sdf.parse("20150717"), sdf.parse("20150721"), sdf.parse("20150722")}, fireDays.toArray());

    }

    static class ExampleJob implements Job {
        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

        }
    }
}
