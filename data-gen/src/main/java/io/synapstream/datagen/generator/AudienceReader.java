package io.synapstream.datagen.generator;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Streams {@link Audience} rows out of a snapshot file, one row group in memory at a time.
 */
final class AudienceReader implements Closeable {

    private final ParquetFileReader file;
    private final MessageColumnIO columnIO;
    private final AudienceMaterializer materializer = new AudienceMaterializer();
    private RecordReader<Audience> rows;
    private long remainingInRowGroup;

    private AudienceReader(ParquetFileReader file) throws IOException {
        this.file = file;
        MessageType schema = file.getFooter().getFileMetaData().getSchema();
        if (!schema.equals(AudienceWriteSupport.SCHEMA)) {
            throw new IOException("Unexpected snapshot schema: " + schema);
        }
        this.columnIO = new ColumnIOFactory().getColumnIO(schema);
    }

    static AudienceReader open(Path path) throws IOException {
        ParquetFileReader file = ParquetFileReader.open(new LocalInputFile(path));
        try {
            return new AudienceReader(file);
        } catch (IOException | RuntimeException e) {
            file.close();
            throw e;
        }
    }

    long rowCount() {
        return file.getRecordCount();
    }

    /** @return the next row, or {@code null} at the end of the file */
    Audience read() throws IOException {
        while (remainingInRowGroup == 0) {
            PageReadStore pages = file.readNextRowGroup();
            if (pages == null) {
                return null;
            }
            rows = columnIO.getRecordReader(pages, materializer);
            remainingInRowGroup = pages.getRowCount();
        }
        remainingInRowGroup--;
        return rows.read();
    }

    @Override
    public void close() throws IOException {
        file.close();
    }

    /** Assembles the three columns of one row into an {@link Audience}. */
    private static final class AudienceMaterializer extends RecordMaterializer<Audience> {
        private long customerId;
        private long segmentId;
        private byte[] hashedUserId;

        private final Converter[] fields = {
                new PrimitiveConverter() {
                    @Override
                    public void addLong(long value) {
                        customerId = value;
                    }
                },
                new PrimitiveConverter() {
                    @Override
                    public void addLong(long value) {
                        segmentId = value;
                    }
                },
                new PrimitiveConverter() {
                    @Override
                    public void addBinary(Binary value) {
                        hashedUserId = value.getBytes();
                    }
                }
        };

        private final GroupConverter root = new GroupConverter() {
            @Override
            public Converter getConverter(int fieldIndex) {
                return fields[fieldIndex];
            }

            @Override
            public void start() {
            }

            @Override
            public void end() {
            }
        };

        @Override
        public Audience getCurrentRecord() {
            return new Audience(customerId, segmentId, hashedUserId);
        }

        @Override
        public GroupConverter getRootConverter() {
            return root;
        }
    }
}
