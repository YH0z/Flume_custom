/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: CustomSink.java
 * File type: JAVA
 * Create time: 2020/6/25 16:28
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.sinks;
import com.mysql.jdbc.Driver;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.Objects;

/**
 * @className CustomSink
 * @author YUAN_HAO
 * @time 2020/6/25 16:28
 * @category
 * @description Custom a Sink
 * @version 1.0
 */
public class CustomSink extends AbstractSink implements Configurable, Serializable {
    private static final long serialVersionUID = 1L;
    // Logger
    private static final Logger logger = LoggerFactory.getLogger(CustomSink.class);

    // Define two attribute of prefix and suffix;
    private String prefix;
    private String suffix;

    @Override
    public void configure(Context context) {
        // Read configuration file, assign a value to prefix and suffix;
        prefix = context.getString("prefix");
        suffix = context.getString("suffix", "default_value");
    }

    /**
     * @methodName process
     * @author YUAN_HAO
     * @date 2020/6/25 17:55
     * @description
     *  1.Get the Channel.
     *  2.Get a Transaction from the Channel.
     *  3.Send a data.
     *
     * @return org.apache.flume.Sink.Status
     */
    @Override
    public Status process() throws EventDeliveryException {
        // Define a return value.
        Status status = null;

        // Get a Channel.
        Channel channel = getChannel();

        // Get a Transaction from the Channel.
        Transaction transaction = channel.getTransaction();

        // Start transaction;
        transaction.begin();

        try {
            // Get a data from the channel.
            Event event = channel.take();

            if (Objects.nonNull(event)) {
                // Handling event.
                // business codes.
                HandlingEvent(event);

            }
            // Submit transaction.
            transaction.commit();

            // Running successfully, change the status to Ready.
            status = Status.READY;
        } catch (Exception e) {
            e.printStackTrace();
            // Roll back the transaction.
            transaction.rollback();
            // Change the status to Back off.
            status = Status.BACKOFF;
        } finally {
            // Finally, close the transaction.
            if (transaction != null) {
                transaction.close();
            }
        }

        return status;
    }

    /**
     * @methodName HandlingEvent
     * @author YUAN_HAO
     * @date 2020/6/25 18:14
     * @description Handling the event.
     * @param event
     * @return void
     */
    private void HandlingEvent(Event event) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, SQLException {
        String body = new String(event.getBody());

        String sql = "INSERT INTO students(student_name) values(?);";
        String password = "5211314abcd";
        String user = "root";
        String url = "jdbc:mysql://192.168.43.63:3306/test";

        // Write data to MySQL.
        boolean executeResult = connectionAndWriteDataToMySQL(url, user, password, sql, body);

        logger.info("Execute result : " + (executeResult ? "successfully" : "failed"));
        logger.info("Wrote to MySQL database, data: " + body);

    }

    /**
     * @methodName connectionAndWriteDataToMySQL
     * @author YUAN_HAO
     * @date 2020/7/4 17:15
     * @description
     * @param url
     * @param user
     * @param password
     * @param sql
     * @param body_data
     * @return boolean
     */
    private boolean connectionAndWriteDataToMySQL(String url, String user, String password, String sql, String body_data) {
        int execute = 0;

        try {
            // Get a connection.
            Connection connection = DriverManager.getConnection(url, user, password);

            // Get a Statement object.
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            // Set a data.
            preparedStatement.setString(1, body_data);

            // Execute the sql command.
            execute = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

        return execute > 0;
    }

    static {
        try {
            // Region a MySQL driver.
            DriverManager.registerDriver(new Driver());
        } catch (SQLException E) {
            throw new RuntimeException("Can't register driver!");
        }
    }

}
