package com.qubole.streamx.s3.wal;

import com.qubole.streamx.s3.S3SinkConnectorConfig;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import io.confluent.connect.hdfs.wal.WAL;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.confluent.connect.hdfs.storage.Storage;

public class DBWAL implements  WAL {
    private static final Logger log = LoggerFactory.getLogger(DBWAL.class);
    String database = "wal";
    String tableName;
    Storage storage;
    Connection connection;
    ArrayList<String> tempFiles = new ArrayList<>();
    ArrayList<String> committedFiles = new ArrayList<>();
    boolean beginMarker = false;
    int partitionId = -1;
    int id = ThreadLocalRandom.current().nextInt(1, 100000 + 1);
    HdfsSinkConnectorConfig config;

    public DBWAL(String logsDir, TopicPartition topicPartition, Storage storage, HdfsSinkConnectorConfig config) {
        this.storage = storage;
        this.config = config;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        partitionId = topicPartition.partition();


        try {
            String name = config.getString(S3SinkConnectorConfig.NAME_CONFIG);
            tableName = name + "_" + topicPartition.topic() + "_" + partitionId;
            
            String connectionURL = config.getString(S3SinkConnectorConfig.DB_CONNECTION_URL_CONFIG);
            String user = config.getString(S3SinkConnectorConfig.DB_USER_CONFIG);
            String password = config.getString(S3SinkConnectorConfig.DB_PASSWORD_CONFIG);
            if(connectionURL.length()==0 || user.length()==0 || password.length()==0)
                throw new ConnectException("db.connection.url,db.user,db.password - all three properties must be specified");
            connection = DriverManager.getConnection(connectionURL, user, password);
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            DatabaseMetaData dbm = connection.getMetaData();

            ResultSet tables = dbm.getTables(null, null, tableName, null);
            if (tables.next()) {
                // No op
            }
            else {
                String sql=String.format("create table %s (id INT AUTO_INCREMENT, tempFile VARCHAR(500), committedFile VARCHAR(500), primary key (id))", tableName);
                log.info("Creating table "+ sql);
                statement.executeUpdate(sql);
                //connection.commit();
            }

        }catch (SQLException e) {
            log.error(e.toString());
            throw new ConnectException(e);

        }
    }

    @Override
    public void acquireLease() throws ConnectException {

        long sleepIntervalMs = 1000L;
        long MAX_SLEEP_INTERVAL_MS = 16000L;
        while (sleepIntervalMs < MAX_SLEEP_INTERVAL_MS) {

            try {
                Statement statement = connection.createStatement();
                statement.setQueryTimeout(5);  // set timeout to 30 sec.
                String sql = String.format("select now() as currentTS,l1.* from l1 where pid = %s for update", partitionId);

                ResultSet rs = statement.executeQuery(sql);
                if(!rs.next()) {
                    sql = String.format("insert into l1(id,pid) values (%s,%s)", id, partitionId);
                    statement.executeUpdate(sql);
                    connection.commit();
                    return;
                }

                if(canAcquireLock(rs)) {
                    sql = String.format("update l1 set id=%s,ts=now() where pid=%s", id, partitionId);
                    statement.executeUpdate(sql);
                    connection.commit();
                    return;
                }
                connection.commit();
            } catch (SQLException e) {
                log.error(e.toString());
                throw new ConnectException(e);
            }
            try {
                Thread.sleep(sleepIntervalMs);
            } catch (InterruptedException ie) {
                throw new ConnectException(ie);
            }
            sleepIntervalMs = sleepIntervalMs * 2;
            if (sleepIntervalMs >= MAX_SLEEP_INTERVAL_MS) {
                throw new ConnectException("Cannot acquire lease after timeout, will retry.");
            }
        }
    }
    private boolean canAcquireLock(ResultSet rs) {
        try {
            boolean exists = rs.next();
            if(!exists)
                return true;
            java.sql.Timestamp now = rs.getTimestamp("currentTS");
            java.sql.Timestamp ts = rs.getTimestamp("ts");

            if(now.getTime() - ts.getTime() >= 60*1000) {
                log.warn("last update is more than a minute" + now + " "+ts);
                return false;
            }
            else {
                log.warn("last update within a minute"+ now + " " + ts);
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }

    public void acquireLease1() throws ConnectException {
        //Implement a lease that keep's renewing as long as thread is alive
        try {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            String sql = String.format("lock table %s write", tableName);
            statement.executeUpdate(sql);
            //ResultSet rs = statement.executeQuery(sql);
            connection.commit();
        } catch (SQLException e) {
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }

    @Override
    public void append(String tempFile, String committedFile) throws ConnectException {
        try {
            acquireLease();

            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            //End Marker - Seen all encoded Partitions, write to DB
            if(beginMarker && committedFile.length()==0) {
                beginMarker = false;
                String tempFilesCommaSeparated = StringUtils.join(",",tempFiles);
                String committedFilesCommaSeparated = StringUtils.join(",",committedFiles);

                String sql = String.format("insert into %s (tempFile,committedFile) values ('%s','%s')", tableName, tempFilesCommaSeparated, committedFilesCommaSeparated);
                log.info("committing " + sql);
                statement.executeUpdate(sql);
                connection.commit();
                return;
            }
            //Begin Marker
            else if(committedFile.length()==0) {
                beginMarker = true;
                tempFiles.clear();
                committedFiles.clear();
                return;
            }
            tempFiles.add(tempFile);
            committedFiles.add(committedFile);
        }catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }

    @Override
    public void apply() throws ConnectException {
        try {
            acquireLease();

            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            String sql = String.format("select * from %s order by id desc limit 1", tableName);
            log.info("Reading wal " + sql);
            ResultSet rs=statement.executeQuery(sql);

            while(rs.next()) {
                String tempFiles = rs.getString("tempFile");
                String committedFiles = rs.getString("committedFile");
                String tempFile[]=tempFiles.split(",");
                String committedFile[]=committedFiles.split(",");
                //TODO : check if all tempFiles are there.
                try {
                    for(int k=0;k<tempFile.length;k++) {
                        storage.commit(tempFile[k], committedFile[k]);
                        log.info("Recovering file "+tempFile[k]+" "+committedFile[k]);
                    }
                } catch (IOException e){
                    e.printStackTrace();
                    throw new ConnectException(e);
                }
            }
        } catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }

    }


    @Override
    public void truncate() throws ConnectException {
        try {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            String sql = String.format("select * from %s order by id desc limit 2", tableName);
            ResultSet rs = statement.executeQuery(sql);
            int rows = 0;
            while(rs.next()){
                rows++;
            }
            if(rows < 2)
                return;
            rs.absolute(2);
            String id = rs.getString("id");
            sql = String.format("delete from %s where id < %s", tableName, id);
            log.info("truncating table " + sql);
            statement.executeUpdate(sql);
            //connection.commit();

        }catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }



    @Override
    public void close() throws ConnectException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new ConnectException("Unable to close connection",e);
        }
    }

    @Override
    public String getLogFile() {
        return tableName;
    }

    @Override
    public long readOffsetFromWAL() {
        ResultSet rs = fetch();
        String committedFiles[];
        long offset = -1L;
        try {
            rs.absolute(1);
            committedFiles = rs.getString("committedFile").split(",");
            boolean lastCommittedRecordExists = checkFileExists(committedFiles);
            if (lastCommittedRecordExists) {
                offset = FileUtils.extractOffset(committedFiles[0]) + 1;
            } else {
                rs.absolute(2);
                committedFiles = rs.getString("committedFile").split(",");
                lastCommittedRecordExists = checkFileExists(committedFiles);
                if (!lastCommittedRecordExists) {
                    throw new ConnectException("Unable to recover");
                }
                offset = FileUtils.extractOffset(committedFiles[0]) + 1;
            }
            log.info("Offset from WAL " + offset +" for topic partition id "+partitionId);

        } catch (SQLException e) {
            log.error("Exception while reading offset from WAL " + e.toString());
        }
        return offset;
    }

    private boolean checkFileExists(String files[]) {
        boolean fileExists = true;
        for(String file: files) {
            try {
                if (!storage.exists(file)) {
                    fileExists = false;
                }
            }catch (IOException e) {
                fileExists = false;
                break;
            }
        }
        return fileExists;
    }

    private ResultSet fetch() throws ConnectException {
        try {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            String sql = String.format("select * from %s order by id desc limit 2", tableName);
            ResultSet rs = statement.executeQuery(sql);
            return rs;
        }catch (SQLException e){
            log.error(e.toString());
            throw new ConnectException(e);
        }
    }
}