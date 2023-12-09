package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

class TaskQueueUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    static String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    static String truncateToUtf8ByteLength(String s, int maxBytes) {
        if (s == null) {
            return null;
        }
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        byte[] sba = s.getBytes(StandardCharsets.UTF_8);
        if (sba.length <= maxBytes) {
            return s;
        }
        // Ensure truncation by having byte buffer = maxBytes
        ByteBuffer bb = ByteBuffer.wrap(sba, 0, maxBytes);
        CharBuffer cb = CharBuffer.allocate(maxBytes);
        // Ignore an incomplete character
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        decoder.decode(bb, cb, true);
        decoder.flush(cb);
        return new String(cb.array(), 0, cb.position());
    }

    static <T> Task<T> convertTaskEntityToTask(TaskEntity taskEntity, Class<T> payloadClass) {
        try {
            return new Task<>(
                    taskEntity.getId(),
                    taskEntity.getAttempt(),
                    OBJECT_MAPPER.readValue(taskEntity.getPayload(), payloadClass)
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed to deserialize JSON string to object. JSON: " + taskEntity.getPayload(), e);
        }
    }

}
