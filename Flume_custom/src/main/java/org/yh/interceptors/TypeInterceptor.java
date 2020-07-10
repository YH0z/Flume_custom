/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: TypeInterceptor.java
 * File type: JAVA
 * Create time: 2020/6/22 11:26
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.interceptors;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @className TypeInterceptor
 * @author YUAN_HAO
 * @time 2020/6/22 11:26
 * @category 
 * @description Custom a interceptor
 * @version 1.0
 */
public class TypeInterceptor implements Serializable, Interceptor {
    private static final long serialVersionUID = 1L;
    // Define a list of store event.
    private List<Event> addHeaderEvents;


    @Override
    public void initialize() {
        // Initializes the list of stored events.
        addHeaderEvents = new ArrayList<>();
    }

    /**
     * @methodName intercept
     * @author YUAN_HAO
     * @date 2020/6/22 11:28
     * @description Intercept a single event.
     * @param event
     * @return org.apache.flume.Event
     */
    @Override
    public Event intercept(Event event) {
        // 1.Get the Header information of the event.
        Map<String, String> headers = event.getHeaders();

        // 2.Get the body information of the event.
        String body = new String(event.getBody());

        // 3.How to add header information depends on whether there is  "hello" in body.
        if (body.contains("hello")) {
            // 4.Add the header information
            headers.put("type", "HAD");
        } else {
            // 4.Add the header information
            headers.put("type", "HAD_NOT");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        // 1.Clean the list.
        addHeaderEvents.clear();

        // 2.Traverse the events to add header information to each event.
        for (Event event: addHeaderEvents) {

            // 3.Add header information to each event.
            Event intercepted_event = intercept(event);
            addHeaderEvents.add(intercept(intercepted_event));
        }

        // 4.Return the result.
        return addHeaderEvents;
    }

    @Override
    public void close() {}

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
