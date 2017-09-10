package xjtu.yaanhyy.spark;

import org.apache.hadoop.hbase.TableName;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import xjtu.yaanhyy.spark.data.User;
import xjtu.yaanhyy.spark.hbase_if.Hbase_if;

import org.apache.hadoop.hbase.client.Connection;


import java.io.IOException;

import static xjtu.yaanhyy.spark.hbase_if.Hbase_if.createTable;
import static xjtu.yaanhyy.spark.hbase_if.Hbase_if.put;
import static xjtu.yaanhyy.spark.hbase_if.Hbase_if.scan;

@RestController

public class DataInput {
    final static Logger logger = Logger.getLogger(DataInput.class);
    @RequestMapping(value="/input",method= RequestMethod.POST)
    User[] data_input(@RequestBody User[] user) throws IOException {
        logger.info("input");
        int num = user.length;
        Connection connection = null;
        try {
            connection = Hbase_if.getConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        TableName tableName = TableName.valueOf("spark");

        //创建HBase表
        Hbase_if.admin = connection.getAdmin();
        if(!Hbase_if.admin.tableExists(tableName)) {
            createTable(connection, tableName, "user");
        }




        //get
       // get(connection, tableName, rowKey);




        //delete
        //deleteTable(connection, tableName);


        for(int i=0; i<num; i++)
        {
            //put

            String rowKey = ""+user[i].user_name;
            put(connection, tableName, rowKey, "user", "name", user[i].user_name );
            put(connection, tableName, rowKey, "user", "age", ""+user[i].age );

        }

        //scan
        scan(connection, tableName);
        return  user;
    }
}
