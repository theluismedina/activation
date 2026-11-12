package io.synapstream.datagen.generator;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Writes rows straight to Parquet's record consumer, avoiding the overhead of the generic Group/Avro object
 * models when producing tens of millions of rows. Two file layouts share this code:
 * <ul>
 *   <li>snapshots: {@code customer_id, segment_id, hashed_user_id}</li>
 *   <li>expected deltas: the same columns plus {@code op} ({@code ADD} or {@code REMOVE})</li>
 * </ul>
 */
abstract sealed class AudienceWriteSupport<T> extends WriteSupport<T> {

    static final String CUSTOMER_ID = "customer_id";
    static final String SEGMENT_ID = "segment_id";
    static final String HASHED_USER_ID = "hashed_user_id";
    static final String OP = "op";

    static final MessageType SCHEMA = Types.buildMessage()
            .required(PrimitiveTypeName.INT64).named(CUSTOMER_ID)
            .required(PrimitiveTypeName.INT64).named(SEGMENT_ID)
            .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(HASHED_USER_ID)
            .named("audience");

    static final MessageType DELTA_SCHEMA = Types.buildMessage()
            .addFields(SCHEMA.getFields().toArray(new Type[0]))
            .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.enumType()).named(OP)
            .named("audience_delta");

    /** A row in an expected-delta file. */
    record DeltaRow(Audience audience, Op op) {
    }

    enum Op {
        ADD, REMOVE;

        final Binary binary = Binary.fromConstantByteArray(name().getBytes(StandardCharsets.US_ASCII));
    }

    private final MessageType schema;
    RecordConsumer consumer;

    private AudienceWriteSupport(MessageType schema) {
        this.schema = schema;
    }

    static ParquetWriter<Audience> snapshotWriter(Path file, WriterSettings settings) throws IOException {
        return configure(new Builder<>(new LocalOutputFile(file), Snapshot::new), settings).build();
    }

    static ParquetWriter<DeltaRow> deltaWriter(Path file, WriterSettings settings) throws IOException {
        return configure(new Builder<>(new LocalOutputFile(file), Delta::new), settings).build();
    }

    private static <T> Builder<T> configure(Builder<T> builder, WriterSettings settings) {
        return builder
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(settings.compression())
                .withRowGroupSize(settings.rowGroupSizeBytes())
                // Hashes are near-unique, so a dictionary only costs CPU before Parquet gives up on it.
                .withDictionaryEncoding(HASHED_USER_ID, false);
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, Map.of());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.consumer = recordConsumer;
    }

    void writeAudienceFields(Audience audience) {
        consumer.startField(CUSTOMER_ID, 0);
        consumer.addLong(audience.customerId());
        consumer.endField(CUSTOMER_ID, 0);

        consumer.startField(SEGMENT_ID, 1);
        consumer.addLong(audience.segmentId());
        consumer.endField(SEGMENT_ID, 1);

        consumer.startField(HASHED_USER_ID, 2);
        consumer.addBinary(Binary.fromConstantByteArray(audience.hashedUserId()));
        consumer.endField(HASHED_USER_ID, 2);
    }

    static final class Snapshot extends AudienceWriteSupport<Audience> {
        Snapshot() {
            super(SCHEMA);
        }

        @Override
        public void write(Audience record) {
            consumer.startMessage();
            writeAudienceFields(record);
            consumer.endMessage();
        }
    }

    static final class Delta extends AudienceWriteSupport<DeltaRow> {
        Delta() {
            super(DELTA_SCHEMA);
        }

        @Override
        public void write(DeltaRow record) {
            consumer.startMessage();
            writeAudienceFields(record.audience());
            consumer.startField(OP, 3);
            consumer.addBinary(record.op().binary);
            consumer.endField(OP, 3);
            consumer.endMessage();
        }
    }

    private static final class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {

        private final Supplier<WriteSupport<T>> writeSupport;

        Builder(OutputFile file, Supplier<WriteSupport<T>> writeSupport) {
            super(file);
            this.writeSupport = writeSupport;
        }

        @Override
        protected Builder<T> self() {
            return this;
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            return writeSupport.get();
        }
    }
}
