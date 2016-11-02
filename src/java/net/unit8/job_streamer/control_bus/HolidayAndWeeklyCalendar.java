package net.unit8.job_streamer.control_bus;

import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

import org.joda.time.LocalTime;
import org.quartz.Calendar;
import org.quartz.impl.calendar.BaseCalendar;

/**
 * @author kawasima
 */
public class HolidayAndWeeklyCalendar extends BaseCalendar implements Calendar, Serializable {
    private boolean[] excludeDays = new boolean[]{true, false, false, false, false, false, true};
    private TreeSet<Date> dates = new TreeSet<Date>();
    private Long dayStart = 0L;

    @Override
    public boolean isTimeIncluded(long timeStamp) {
        // Test the base calendar first. Only if the base calendar not already
        // excludes the time/date, continue evaluating this calendar instance.
        if (!super.isTimeIncluded(timeStamp)) { return false; }

        java.util.Calendar cl = createJavaCalendar(timeStamp - dayStart);
        int wday = cl.get(java.util.Calendar.DAY_OF_WEEK);
        Date lookFor = getStartOfDayJavaCalendar(timeStamp - dayStart).getTime();

        return !(excludeDays[wday - 1]) && !(dates.contains(lookFor));
    }


    /**
     * <p>
     * Determine the next time (in milliseconds) that is 'included' by the
     * Calendar after the given time. Return the original value if timeStamp is
     * included. Return 0 if all days are excluded.
     * </p>
     *
     * <p>
     * Note that this Calendar is only has full-day precision.
     * </p>
     */
    @Override
    public long getNextIncludedTime(long timeStamp) {
        if (excludeDays[0] && excludeDays[1] && excludeDays[2] && excludeDays[3] &&
                excludeDays[4] && excludeDays[5] && excludeDays[6]) {
            return 0;
        }
        // Call base calendar implementation first
        long baseTime = super.getNextIncludedTime(timeStamp);
        if ((baseTime > 0) && (baseTime > timeStamp)) {
            timeStamp = baseTime;
        }

        // Get timestamp for 00:00:00
        java.util.Calendar cl = getStartOfDayJavaCalendar(timeStamp - dayStart);
        int wday = cl.get(java.util.Calendar.DAY_OF_WEEK);

        if (!excludeDays[wday - 1]) {
            return timeStamp; // return the original value
        }

        while (!isTimeIncluded(cl.getTime().getTime())) {
            cl.add(java.util.Calendar.DATE, 1);
        }

        return cl.getTime().getTime();
    }

    /**
     * <p>
     * Redefine the array of days excluded. The array must of size greater or
     * equal 8. java.util.Calendar's constants like MONDAY should be used as
     * index. A value of true is regarded as: exclude it.
     * </p>
     */
    public void setDaysExcluded(boolean[] weekDays) {
        if (weekDays == null) {
            return;
        }

        excludeDays = weekDays;
    }

    /**
     * <p>
     * Add the given Date to the list of excluded days. Only the month, day and
     * year of the returned dates are significant.
     * </p>
     */
    public void addExcludedDate(Date excludedDate) {
        Date date = getStartOfDayJavaCalendar(excludedDate.getTime()).getTime();
        /*
         * System.err.println( "HolidayCalendar.add(): date=" +
         * excludedDate.toLocaleString());
         */
        this.dates.add(date);
    }

    public void removeExcludedDate(Date dateToRemove) {
        Date date = getStartOfDayJavaCalendar(dateToRemove.getTime()).getTime();
        dates.remove(date);
    }

    /**
     * <p>
     * Returns a <code>SortedSet</code> of Dates representing the excluded
     * days. Only the month, day and year of the returned dates are
     * significant.
     * </p>
     */
    public SortedSet<Date> getExcludedDates() {
        return Collections.unmodifiableSortedSet(dates);
    }

    public void setDayStart(Long dayStart){
        this.dayStart = dayStart;
    }

}
