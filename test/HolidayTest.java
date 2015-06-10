import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.calendar.HolidayCalendar;
import org.quartz.spi.OperableTrigger;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @kawasima
 */
public class HolidayTest {
    @Test
    public void test() throws ParseException, SchedulerException {
        HolidayCalendar holidayCalendar = new HolidayCalendar();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        holidayCalendar.addExcludedDate(sdf.parse("20150429"));
        holidayCalendar.addExcludedDate(sdf.parse("20150503"));
        holidayCalendar.addExcludedDate(sdf.parse("20150504"));
        holidayCalendar.addExcludedDate(sdf.parse("20150505"));

        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        Scheduler scheduler = schedulerFactory.getScheduler();
        scheduler.addCalendar("holidays", holidayCalendar, false, false);

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
                                .startingDailyAt(TimeOfDay.hourAndMinuteOfDay(3, 5))
                                .onMondayThroughFriday())
                .startAt(sdf.parse("201504028"))
                .modifiedByCalendar("holidays")
                .build();
        scheduler.scheduleJob(jobDetail, trigger);

        System.out.println(TriggerUtils.computeFireTimes((OperableTrigger)trigger, holidayCalendar, 10));
    }

    static class ExampleJob implements Job {
        @Override
        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

        }
    }
}
