package com.hevodata.scheduler.helpers.profile;

public class LocalMySql extends MySqlProfile {
    public LocalMySql() {
        super("localhost", 3306, "root", "abcd1234", "alfred");
    }
}
