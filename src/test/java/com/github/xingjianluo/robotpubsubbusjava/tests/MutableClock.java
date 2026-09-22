package com.github.xingjianluo.robotpubsubbusjava.tests;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

/** 测试用可变时钟，用于确定性控制确认超时。 */
final class MutableClock extends Clock {

    private final AtomicLong millis = new AtomicLong(0);

    void advance(long deltaMillis) {
        millis.addAndGet(deltaMillis);
    }

    @Override
    public long millis() {
        return millis.get();
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis.get());
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }
}
