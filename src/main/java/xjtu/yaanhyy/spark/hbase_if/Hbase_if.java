package xjtu.yaanhyy.spark.hbase_if;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;


public class Hbase_if {
    final static Logger logger = Logger.getLogger(Hbase_if.class);
    static public Admin admin = null;
    public static Properties load(File file) throws IOException {
        InputStream in = null;
        try {
            in = new FileInputStream(file);
            Properties props = new Properties();
            props.load(in);
            return props;
        }finally{
            //IoUtils.closeQuietly(in);
        }
    }

    public static Properties load(String path) throws IOException{
        InputStream in = null;
        try {
            in = ClassUtils.getClassLoader().getResourceAsStream(path);
            Properties props = new Properties();
            props.load(in);
            return props;
        }finally{
            //IoUtils.closeQuietly(in);
        }
    }

    public static Connection getConnection() throws IOException {
        Connection connection = ConnectionFactory.createConnection(getConfiguration());
        return connection;
    }

    private static Configuration getConfiguration() throws IOException {

        Properties props = load("hbase.properties");

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", props.getProperty("hbase.zookeeper.property.clientPort"));
        config.set("hbase.zookeeper.quorum", props.getProperty("hbase.zookeeper.quorum"));
        return config;
    }

    public static void createTable(Connection connection, TableName tableName, String... columnFamilies) throws IOException {

        try {


                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                for(String columnFamily : columnFamilies) {
                    tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                }
                admin.createTable(tableDescriptor);
                logger.info("create table:{} success!"+ tableName.getName());

        } finally {
            if(admin!=null) {
                admin.close();
            }
        }
    }

    public static void scan(Connection connection, TableName tableName) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            ResultScanner rs = null;
            try {
                //Scan scan = new Scan(Bytes.toBytes("u120000"), Bytes.toBytes("u200000"));
                rs = table.getScanner(new Scan());
                for(Result r:rs){
                    KeyValue[] kvs=r.raw();
                    for(KeyValue kv:kvs)
                    {
                        System.out.println(Bytes.toString(kv.getRow()));
                        System.out.println(Bytes.toString(kv.getFamily()));
                        System.out.println(Bytes.toString(kv.getQualifier()));
                        System.out.println(Bytes.toString(kv.getValue()));
                    }
                }
            } finally {
                if(rs!=null) {
                    rs.close();
                }
            }
        } finally {
            if(table!=null) {
                table.close();
            }
        }
    }

    /**批量插入可以使用 Table.put(List<Put> list)**/
    public static  void put(Connection connection, TableName tableName,
                    String rowKey, String columnFamily, String column, String data) throws IOException {

        Table table = null;
        try {
            table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(put);
        } finally {
            if(table!=null) {
                table.close();
            }
        }
    }

}
