package edu.msu.cse.dkvf;

import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import java.util.logging.ConsoleHandler;

/**
 * The handler class to send logs to both Standard output and error based on a boundary.
 */
public class DualConsoleHandler extends StreamHandler {

    private final ConsoleHandler stderrHandler = new ConsoleHandler();
    private Level least;
    private Level errorBoundary;

    /**
     * @param least         The minimum level to log
     * @param errorBoundary The boundary to send logs to error instead of our.
     *                      Anything <i>above</i> boundary will send to error.
     */
    public DualConsoleHandler(Level least, Level errorBoundary) {
        super(System.out, new SimpleFormatter());
        super.setLevel(least);
        this.least = least;
        this.errorBoundary = errorBoundary;
    }

    @Override
    public void publish(LogRecord record) {

        if (record.getLevel().intValue() >= least.intValue()) {
            if (record.getLevel().intValue() <= errorBoundary.intValue()) {
                super.publish(record);
                super.flush();
            } else {
                stderrHandler.publish(record);
                stderrHandler.flush();
            }
        }
    }
}