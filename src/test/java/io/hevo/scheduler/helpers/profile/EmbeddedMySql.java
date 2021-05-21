package io.hevo.scheduler.helpers.profile;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.ScriptResolver;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.distribution.Version;

import java.util.concurrent.ThreadLocalRandom;

public class EmbeddedMySql extends MySqlProfile {

    private EmbeddedMysql embeddedMysql;

    public EmbeddedMySql() {
        super("localhost", ThreadLocalRandom.current().nextInt(10_000, 30_000), "root_", "password", "test_schema");
    }

    @Override
    public void start() {
        MysqldConfig config = MysqldConfig.aMysqldConfig(Version.v5_7_latest).withPort(port).withUser(user, password).build();
        this.embeddedMysql = EmbeddedMysql.anEmbeddedMysql(config).addSchema(schema, ScriptResolver.classPathScript("db/001.sql")).start();
    }

    @Override
    public void stop() {
        this.embeddedMysql.stop();
    }
}
