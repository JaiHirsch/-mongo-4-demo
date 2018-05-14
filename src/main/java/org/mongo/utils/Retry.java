package org.mongo.utils;

import java.util.ArrayList;
import java.util.List;

public class Retry {

    private int allowedAttempts;
    private int timesAttempted = 0;
    private boolean complete = false;
    private long delay = 0L;

    private List <Throwable> exceptions;

    public Retry() {
        exceptions = new ArrayList <Throwable>();
    }

    public Retry withAttempts(int numAttempts) {
        this.allowedAttempts = numAttempts;
        return this;
    }

    public boolean shouldContinue() {
        if (!complete && timesAttempted != 0)
            try {
                Thread.sleep(delay);
            }
            catch (InterruptedException e) {
                /* nada. This makes it so your methods
                 * that use this class don't have to
                 * Throw an InterruptedException */
            }

        return !complete && (allowedAttempts > timesAttempted++);
    }

    public int getTimesAttempted() {
        return timesAttempted;
    }



    public void markAsComplete() {
        complete = true;
    }

    public void takeException(Throwable e) {
        exceptions.add(e);
    }

    public List <Throwable> getExceptions() {
        return exceptions;
    }

    public boolean completedOk() {
        return complete;
    }


    public Retry withDelay(long delay) {
        this.delay = delay;
        return this;
    }

    public Throwable getLastException() {
        return exceptions.get(exceptions.size() - 1);
    }

}
