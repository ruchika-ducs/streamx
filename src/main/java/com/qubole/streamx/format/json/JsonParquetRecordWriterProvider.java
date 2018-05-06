package com.qubole.streamx.format.json;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qubole.streamx.s3.S3SinkConnector;
import com.qubole.streamx.s3.S3SinkConnectorConfig;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;

/**
 * Needs to be used in conjunction with the {@link org.apache.kafka.connect.storage.StringConverter}, which is set via the
 * config/connect-*.properties key.converter and value.converter properties.
 */
public class JsonParquetRecordWriterProvider implements RecordWriterProvider {

    private static final Logger log = LoggerFactory.getLogger(JsonParquetRecordWriterProvider.class);

    @Override
    public String getExtension() {
        return ".snappy.parquet";
    }

    @Override
    public RecordWriter<SinkRecord> getRecordWriter(Configuration conf, String fileName, SinkRecord record, AvroData avroData) throws IOException {
        Path path = new Path(fileName);

        String schemaString = S3SinkConnector.getConfigString(S3SinkConnectorConfig.PARQUET_SCHEMA_CONFIG);
        if (schemaString == null) {
            throw new IllegalArgumentException(String.format("A Parquet schema must be specified under property %s!",
                    S3SinkConnectorConfig.PARQUET_SCHEMA_CONFIG));
        }

        MessageType schema = MessageTypeParser.parseMessageType(schemaString);
        log.debug("Schema String = {}", schema.toString());

        final SourceParquetOutputFormat sourceParquetOutputFormat = new SourceParquetOutputFormat(schema);
        final org.apache.hadoop.mapreduce.RecordWriter<Void, Object> recordWriter;
        try {
            recordWriter = sourceParquetOutputFormat.getRecordWriter(conf, path, CompressionCodecName.SNAPPY);
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
        return new RecordWriter<SinkRecord>() {
            @Override
            public void write(SinkRecord sinkRecord) throws IOException {
                try {
                    log.trace("SinkRecord = {}", sinkRecord.value());
                    recordWriter.write(null, sinkRecord.value().toString());
                } catch (InterruptedException ie) {
                    throw new IOException(ie);
                }
            }

            @Override
            public void close() throws IOException {
                try {
                    recordWriter.close(null);
                } catch (InterruptedException ie) {
                    throw new IOException(ie);
                }
            }
        };
    }

    /**
     * Used to parse and write each json record to the Parquet {@link RecordConsumer}.
     */
    private static final class JsonParquetWriteSupport extends WriteSupport<Object> {

        private static final Logger log = LoggerFactory.getLogger(JsonParquetWriteSupport.class);

        private final MessageType schema;

        private RecordConsumer recordConsumer;

        public JsonParquetWriteSupport(MessageType schema) {
            this.schema = schema;
        }

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(schema, new HashMap<String, String>());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(Object record) {
        	log.info("Recod wil be:{}", record.toString());
            /*String[] csvRecords = record.split(",");

            if (csvRecords.length > 0) {
                recordConsumer.startMessage();

                for (int i = 0; i < columns.size(); i++) {
                    ColumnDescriptor columnDescriptor = columns.get(i);

                    // If there aren't enough entries in the csvRecords, write an empty string
                    String csvRecord = (csvRecords.length < i) ? "" : csvRecords[i];

                    recordConsumer.startField(columns.get(i).getPath()[0], i);
                    switch (columnDescriptor.getType()) {
                        case INT32:
                            recordConsumer.addInteger(Integer.parseInt(csvRecord));
                            break;
                        case INT64:
                        case INT96:
                            recordConsumer.addLong(Long.parseLong(csvRecord));
                            break;
                        case BINARY:
                        case FIXED_LEN_BYTE_ARRAY:
                            recordConsumer.addBinary(Binary.fromString(csvRecord));
                            break;
                        case BOOLEAN:
                            recordConsumer.addBoolean(Boolean.parseBoolean(csvRecord));
                            break;
                        case FLOAT:
                            recordConsumer.addFloat(Float.parseFloat(csvRecord));
                            break;
                        case DOUBLE:
                            recordConsumer.addDouble(Double.parseDouble(csvRecord));
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    String.format("Unsupported record conversion for type '%s'!", columnDescriptor.getType().name()));
                    }

                    recordConsumer.endField(columns.get(i).getPath()[0], i);
                }
                recordConsumer.endMessage();*/
            }
        }
    

    private final class SourceParquetOutputFormat extends ParquetOutputFormat<Object> {
        private SourceParquetOutputFormat(MessageType schema) {
            super(new JsonParquetWriteSupport(schema));
        }
    }

}
