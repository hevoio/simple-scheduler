package io.hevo.scheduler.helpers.profile;

public abstract class MySqlProfile {
    public final String host;
    public final int port;
    public final String user;
    public final String password;
    public final String schema;

    public MySqlProfile(String host, int port, String user, String password, String schema) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.schema = schema;
    }

    public void start() {}
    public void stop() {}
}
