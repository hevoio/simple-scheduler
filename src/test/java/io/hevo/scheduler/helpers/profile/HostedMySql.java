package io.hevo.scheduler.helpers.profile;

public class HostedMySql extends MySqlProfile {
    public HostedMySql(String host, int port, String user, String password, String schema) {
        super(host, port, user, password, schema);
    }
}
