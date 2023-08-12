package main.java.com.server.config;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

@Configuration
public class HBaseConfig {

    @Bean
    public HBaseConnectionFactory hBaseFactory() throws IOException {
        org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", "quickstart-bigdata"); // Replace with actual ZooKeeper quorum
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181"); // Replace with actual ZooKeeper client port
        hbaseConfig.set("zookeeper.znode.parent", "/hbase"); // Optional, specify the ZooKeeper znode parent

        return () -> ConnectionFactory.createConnection(hbaseConfig);
    }
}