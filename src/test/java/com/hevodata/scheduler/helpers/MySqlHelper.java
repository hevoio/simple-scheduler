package com.hevodata.scheduler.helpers;

import com.hevodata.scheduler.helpers.profile.MySqlProfile;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;

public class MySqlHelper {

    public final MySqlProfile mySqlProfile;

    public MySqlHelper(MySqlProfile mySqlProfile) {
        this.mySqlProfile = mySqlProfile;
    }

    public DataSource createDataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUsername(mySqlProfile.user);
        dataSource.setPassword(mySqlProfile.password);
        dataSource.setUrl(String.format("jdbc:mysql://%s:%d/%s", mySqlProfile.host, mySqlProfile.port, mySqlProfile.schema));
        dataSource.setMaxTotal(3);
        dataSource.setMaxIdle(1);
        dataSource.setInitialSize(3);
        dataSource.setValidationQuery("SELECT 1");

        return dataSource;
    }
}
