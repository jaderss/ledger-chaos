package dev.jaderss.chaos;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantLock;
import tools.jackson.databind.json.JsonMapper;

final class JsonLog implements AutoCloseable {

    record Body(String from, String to, long amount, String currency) {

        Body plusOne() {
            return new Body(from, to, amount + 1, currency);
        }
    }

    record Entry(
            long t,
            int worker,
            String op,
            String key,
            String phase,
            @JsonInclude(JsonInclude.Include.NON_NULL) Body body,
            @JsonInclude(JsonInclude.Include.NON_DEFAULT) boolean mismatch,
            @JsonInclude(JsonInclude.Include.NON_DEFAULT) int status,
            @JsonInclude(JsonInclude.Include.NON_EMPTY) String transferId,
            @JsonInclude(JsonInclude.Include.NON_EMPTY) String transferStatus,
            @JsonInclude(JsonInclude.Include.NON_EMPTY) String problem,
            @JsonInclude(JsonInclude.Include.NON_DEFAULT) boolean replayed,
            @JsonInclude(JsonInclude.Include.NON_EMPTY) String error) {

        static Entry invoke(int worker, String key, Body body, boolean mismatch) {
            return new Entry(
                    System.currentTimeMillis(),
                    worker,
                    "transfer",
                    key,
                    "invoke",
                    body,
                    mismatch,
                    0,
                    null,
                    null,
                    null,
                    false,
                    null);
        }

        static Entry result(int worker, String key, boolean mismatch, Outcome o) {
            return new Entry(
                    System.currentTimeMillis(),
                    worker,
                    "transfer",
                    key,
                    o.phase(),
                    null,
                    mismatch,
                    o.status(),
                    o.transferId(),
                    o.transferStatus(),
                    o.problem(),
                    o.replayed(),
                    o.error());
        }
    }

    record FaultEntry(
            long t,
            String fault,
            String phase,
            @JsonInclude(JsonInclude.Include.NON_EMPTY) String error) {

        static FaultEntry of(String fault, String phase, String error) {
            return new FaultEntry(System.currentTimeMillis(), fault, phase, error);
        }
    }

    private final BufferedWriter writer;
    private final JsonMapper json;
    private final ReentrantLock lock = new ReentrantLock();

    JsonLog(Path path, JsonMapper json) throws IOException {
        this.writer = Files.newBufferedWriter(path);
        this.json = json;
    }

    void write(Object entry) {
        var line = json.writeValueAsString(entry);
        lock.lock();
        try {
            writer.write(line);
            writer.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            writer.close();
        } finally {
            lock.unlock();
        }
    }
}
