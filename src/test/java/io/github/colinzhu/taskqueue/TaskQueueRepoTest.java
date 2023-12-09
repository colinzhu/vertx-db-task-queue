package io.github.colinzhu.taskqueue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class TaskQueueRepoTest {
    private Method method;

    @BeforeEach
    void setUp() throws Exception {
        method = TaskQueueRepo.class.getDeclaredMethod("truncateToUtf8ByteLength", String.class, int.class);
        method.setAccessible(true);
    }

    @Test
    void testTruncateToUtf8ByteLength_withChineseCharacters_exceedLength() throws Exception {
        String input = "测试测试测试测试!";
        String expected = "测试测";
        int length = 10;

        String actual = invokeMethod(input, length);

        assertByteLength(actual, length);
        assertEquals(expected, actual);
    }

    @Test
    void testTruncateToUtf8ByteLength_withChineseCharacters_withinLength() throws Exception {
        String input = "测试测";
        String expected = "测试测";
        int length = 10;

        String actual = invokeMethod(input, length);

        assertByteLength(actual, length);
        assertEquals(expected, actual);
    }

    @Test
    void testTruncateToUtf8ByteLength_withNullInput() throws Exception {
        String actual = invokeMethod(null, 10);

        assertNull(actual);
    }

    @Test
    void testTruncateToUtf8ByteLength_withEnglishCharacters_withinLength() throws Exception {
        String input = "Hello";
        String expected = "Hello";
        int length = 10;

        String actual = invokeMethod(input, length);

        assertByteLength(actual, length);
        assertEquals(expected, actual);
    }

    @Test
    void testTruncateToUtf8ByteLength_withEnglishCharacters_exceedLength() throws Exception {
        String input = "HelloHelloHelloHelloHello";
        String expected = "HelloHello";
        int length = 10;

        String actual = invokeMethod(input, length);

        assertByteLength(actual, length);
        assertEquals(expected, actual);
    }

    private String invokeMethod(String input, int length) throws Exception {
        return (String) method.invoke(TaskQueueRepo.getInstance(), input, length);
    }

    private void assertByteLength(String actual, int length) {
        byte[] actualBytes = actual.getBytes();
        assertTrue(actualBytes.length <= length, "Actual byte length is greater than the given length");
    }
}