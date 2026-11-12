package io.synapstream.datagen.generator;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import java.util.Map;

/**
 * Writes {@link Audience} rows straight to Parquet's record consumer, avoiding the overhead of the generic
 * Group/Avro object models when producing tens of millions of rows.
 */
final class AudienceWriteSupport extends WriteSupport<Audience> {

    static final String CUSTOMER_ID = "customer_id";
    static final String SEGMENT_ID = "segment_id";
    static final String HASHED_USER_ID = "hashed_user_id";

    static final MessageType SCHEMA = Types.buildMessage()
            .required(PrimitiveTypeName.INT64).named(CUSTOMER_ID)
            .required(PrimitiveTypeName.INT64).named(SEGMENT_ID)
            .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(HASHED_USER_ID)
            .named("audience");

    private RecordConsumer consumer;

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(SCHEMA, Map.of());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.consumer = recordConsumer;
    }

    @Override
    public void write(Audience record) {
        consumer.startMessage();

        consumer.startField(CUSTOMER_ID, 0);
        consumer.addLong(record.customerId());
        consumer.endField(CUSTOMER_ID, 0);

        consumer.startField(SEGMENT_ID, 1);
        consumer.addLong(record.segmentId());
        consumer.endField(SEGMENT_ID, 1);

        consumer.startField(HASHED_USER_ID, 2);
        consumer.addBinary(Binary.fromConstantByteArray(record.hashedUserId()));
        consumer.endField(HASHED_USER_ID, 2);

        consumer.endMessage();
    }

    static final class Builder extends ParquetWriter.Builder<Audience, Builder> {

        Builder(OutputFile file) {
            super(file);
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<Audience> getWriteSupport(Configuration conf) {
            return new AudienceWriteSupport();
        }
    }
}
