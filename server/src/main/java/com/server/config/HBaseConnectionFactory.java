package main.java.com.server.config;

import org.apache.hadoop.hbase.client.Connection;

import javax.security.auth.login.LoginException;
import java.io.IOException;

@FunctionalInterface
public interface HBaseConnectionFactory {
    Connection connect() throws IOException, LoginException;
}
