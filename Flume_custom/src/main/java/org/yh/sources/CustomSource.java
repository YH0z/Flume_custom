/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: CustomSource.java
 * File type: JAVA
 * Create time: 2020/6/22 15:55
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.sources;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yh.sinks.CustomSink;

import java.io.Serializable;

/**
 * @className CustomSource
 * @author YUAN_HAO
 * @time 2020/6/22 15:55
 * @category 
 * @description Custom a source
 * @version 1.0
 */
public class CustomSource extends AbstractSource implements Serializable, Configurable, PollableSource {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(CustomSink.class);

    // Define global prefix and suffix
    private String prefix;
    private String suffix;


    @Override
    public void configure(Context context) {
        // Read configuration information, Assign a value to prefix and suffix.
        prefix = context.getString("prefix");
        suffix = context.getString("suffix", "DEFAULT_VALUE");
    }

    /**
     * @methodName process
     * @author YUAN_HAO
     * @date 2020/6/22 16:07
     * @description
     *  1.Create data by for loop
     *  2.Encapsulate as event
     *  3.Transmit event to the channel
     * @return org.apache.flume.PollableSource.Status
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        try {
            // 1.Receive data.
            for (int i = 0; i < 10; i++) {
				// 2.Build the event object.
				SimpleEvent event = new SimpleEvent();

				// 3.Set a value of the event.
				event.setBody((prefix + "--" + i + "--" + suffix).getBytes());

				// 4.Transmit event to the channel
				getChannelProcessor().processEvent(event);
			}
			status=  Status.READY;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            status = Status.BACKOFF;
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
