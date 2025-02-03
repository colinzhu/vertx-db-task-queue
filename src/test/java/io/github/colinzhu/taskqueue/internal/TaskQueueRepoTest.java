package io.github.colinzhu.taskqueue.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TaskQueueRepoTest {

    @Test
    void testTruncateToUtf8ByteLength_withChineseCharacters_exceedLength() {
        String input = "测试测试测试测试!";
        String expected = "测试测";
        int length = 10;

        String actual = TaskQueueRepo.truncateToUtf8ByteLength(input, length);

        assertByteLength(actual, length);
        assertEquals(expected, actual);
    }

    @Test
    void testTruncateToUtf8ByteLength_withChineseCharacters_withinLength() {
        String input = "测试测";
        String expected = "测试测";
        int length = 10;

        String actual = TaskQueueRepo.truncateToUtf8ByteLength(input, length);

        assertByteLength(actual, length);
        assertEquals(expected, actual);
    }

    @Test
    void testTruncateToUtf8ByteLength_withNullInput() {
        assertNull(TaskQueueRepo.truncateToUtf8ByteLength(null, 10));
    }

    @Test
    void testTruncateToUtf8ByteLength_withEnglishCharacters_withinLength() {
        String input = "Hello";
        String expected = "Hello";
        int length = 10;

        String actual = TaskQueueRepo.truncateToUtf8ByteLength(input, length);

        assertByteLength(actual, length);
        assertEquals(expected, actual);
    }

    @Test
    void testTruncateToUtf8ByteLength_withEnglishCharacters_exceedLength() {
        String input = "HelloHelloHelloHelloHello";
        String expected = "HelloHello";
        int length = 10;

        String actual = TaskQueueRepo.truncateToUtf8ByteLength(input, length);

        assertByteLength(actual, length);
        assertEquals(expected, actual);
    }

    private void assertByteLength(String actual, int length) {
        byte[] actualBytes = actual.getBytes();
        assertTrue(actualBytes.length <= length, "Actual byte length is greater than the given length");
    }
}