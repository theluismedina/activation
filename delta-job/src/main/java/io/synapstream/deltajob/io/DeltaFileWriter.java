package io.synapstream.deltajob.io;

import io.synapstream.deltajob.model.DeltaManifest.CustomerRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Writes one delta file from rows arriving in (customer_id, segment_id, op, hashed_user_id) order, and records the
 * per-customer row ranges as it goes.
 * <p>
 * The schema is the same as data-gen's expected-delta files, so the two can be compared row for row. Writing goes
 * straight to a local file rather than through Hadoop's FileSystem, which needs winutils on Windows.
 */
public final class DeltaFileWriter implements Closeable {

    public static final String CUSTOMER_ID = "customer_id";
    public static final String SEGMENT_ID = "segment_id";
    public static final String HASHED_USER_ID = "hashed_user_id";
    public static final String OP = "op";
    public static final String ADD = "ADD";
    public static final String REMOVE = "REMOVE";

    static final MessageType SCHEMA = Types.buildMessage()
            .required(PrimitiveTypeName.INT64).named(CUSTOMER_ID)
            .required(PrimitiveTypeName.INT64).named(SEGMENT_ID)
            .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(HASHED_USER_ID)
            .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.enumType()).named(OP)
            .named("audience_delta");

    private static final Binary ADD_BINARY = Binary.fromConstantByteArray(ADD.getBytes(StandardCharsets.US_ASCII));
    private static final Binary REMOVE_BINARY =
            Binary.fromConstantByteArray(REMOVE.getBytes(StandardCharsets.US_ASCII));

    /** One changed row. */
    public record Change(long customerId, long segmentId, String hashedUserId, boolean add) {
    }

    private final ParquetWriter<Change> writer;
    private final List<CustomerRange> customers = new ArrayList<>();
    private long rows;
    private long added;
    private long removed;
    private long currentCustomer = Long.MIN_VALUE;
    private long currentFirstRow;
    private long currentAdded;
    private long currentRemoved;

    public DeltaFileWriter(Path file, CompressionCodecName compression, long rowGroupSizeBytes) throws IOException {
        this.writer = new Builder(new LocalOutputFile(file))
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(compression)
                .withRowGroupSize(rowGroupSizeBytes)
                // Hashes are near-unique, so a dictionary only costs CPU before Parquet gives up on it.
                .withDictionaryEncoding(HASHED_USER_ID, false)
                .build();
    }

    public void write(Change change) throws IOException {
        if (change.customerId() != currentCustomer) {
            if (change.customerId() < currentCustomer) {
                throw new IllegalStateException("Rows must arrive sorted by customer_id: got %d after %d"
                        .formatted(change.customerId(), currentCustomer));
            }
            closeCustomer();
            currentCustomer = change.customerId();
            currentFirstRow = rows;
        }
        writer.write(change);
        rows++;
        if (change.add()) {
            added++;
            currentAdded++;
        } else {
            removed++;
            currentRemoved++;
        }
    }

    private void closeCustomer() {
        if (currentCustomer != Long.MIN_VALUE) {
            customers.add(new CustomerRange(currentCustomer, currentFirstRow, rows - currentFirstRow, currentAdded,
                    currentRemoved));
        }
        currentAdded = 0;
        currentRemoved = 0;
    }

    @Override
    public void close() throws IOException {
        closeCustomer();
        currentCustomer = Long.MIN_VALUE;
        writer.close();
    }

    public long rows() {
        return rows;
    }

    public long added() {
        return added;
    }

    public long removed() {
        return removed;
    }

    /** Valid after {@link #close()}. */
    public List<CustomerRange> customers() {
        return List.copyOf(customers);
    }

    private static final class Builder extends ParquetWriter.Builder<Change, Builder> {

        Builder(OutputFile file) {
            super(file);
        }

        @Override
        protected Builder self() {
            return this;
        }

        // Still the abstract method to implement; its ParquetConfiguration replacement delegates to it.
        @SuppressWarnings("deprecation")
        @Override
        protected WriteSupport<Change> getWriteSupport(Configuration conf) {
            return new ChangeWriteSupport();
        }
    }

    private static final class ChangeWriteSupport extends WriteSupport<Change> {

        private RecordConsumer consumer;

        @SuppressWarnings("deprecation")
        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(SCHEMA, Map.of());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.consumer = recordConsumer;
        }

        @Override
        public void write(Change change) {
            consumer.startMessage();
            field(0, CUSTOMER_ID, change.customerId());
            field(1, SEGMENT_ID, change.segmentId());
            consumer.startField(HASHED_USER_ID, 2);
            consumer.addBinary(Binary.fromString(change.hashedUserId()));
            consumer.endField(HASHED_USER_ID, 2);
            consumer.startField(OP, 3);
            consumer.addBinary(change.add() ? ADD_BINARY : REMOVE_BINARY);
            consumer.endField(OP, 3);
            consumer.endMessage();
        }

        private void field(int index, String name, long value) {
            consumer.startField(name, index);
            consumer.addLong(value);
            consumer.endField(name, index);
        }
    }
}
