package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.apache.bookkeeper.bookie.BufferedChannelReadTest.WriteBeforeReadingType.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

@RunWith(value = Parameterized.class)
public class BufferedChannelReadTest {
    private BufferedChannel bufferedChannel;
    private final long position; // {<0, =>0}
    private final int length; // {<0, =>0}
    private final int numWriteBytes;
    private final int numReadBytes;
    private final boolean isNotNullByteBuff;
    private final Object isExceptionExpected;

    public BufferedChannelReadTest(int capacity, int numberByteWritten, int numReadBytes, long position, int length, WriteBeforeReadingType writeBeforeReading, FileChannel fileChannel, boolean isNotNullByteBuff, Object isExceptionExpected) throws IOException {
        this.position = position;
        this.length = length;
        this.numWriteBytes = numberByteWritten;
        this.numReadBytes = numReadBytes;
        this.isNotNullByteBuff = isNotNullByteBuff;
        this.isExceptionExpected = isExceptionExpected;
        configure(capacity, numberByteWritten, writeBeforeReading, fileChannel);
    }

    private void configure(int capacity, int numWriteBytes, WriteBeforeReadingType writeBeforeReading, FileChannel fileChannel) throws IOException {
        this.bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel, capacity);
        switch (writeBeforeReading) {
            case WRITE:
                this.bufferedChannel.write(createByteBuff(numWriteBytes));
            case NOT_WRITE:
                break;
            case MOCK:
                this.bufferedChannel = new BufferedChannel(mockByteBufAllocator(), fileChannel, capacity);
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() throws IOException {
        return Arrays.asList(new Object[][]{
                //capacity, numWriteBytes, numReadBytes, pos (read), length (read)
                //ByteBuff null
                {15, 2, 0, 0L, 2, NOT_WRITE, returnFileChannel(), true, NullPointerException.class}, // NullPointerException.class
                //ByteBuff not null
                {15, 2, 2, -1L, 3, WRITE, returnFileChannel(), false, IllegalArgumentException.class},
                {15, 2, 2, 20L, 3, WRITE, returnFileChannel(), false, IllegalArgumentException.class},
                {15, 2, 2, 0L, -2, WRITE, returnFileChannel(), false, false},
                {15, 2, 2, 0L, 5, WRITE, returnFileChannel(), false, IOException.class},
                // not writing before reading
                //{0, 2, 2, 0L, 2, NOT_WRITE, returnFileChannel(), false, IOException.class},
                //{15, 2, 2, 0L, 2, NOT_WRITE, mockFileChannel(5), false, false},
//
                //{15, 2, 2, 5L, 1, WRITE, returnFileChannel(), false, IllegalArgumentException.class}, // writeBufferStartPosition < position
                //{15, 2, 2, -1L, 1, WRITE, returnFileChannel(), false, IllegalArgumentException.class}, // writeBufferStartPosition > position
                //{15, 2, 2, 0L, 2, WRITE, returnFileChannel(), false, false},// writeBufferStartPosition = position
                //// writeBuffer = null
                //{15, 2, 2, 5L, 2, MOCK, mockFileChannel(5), false, false}, // writeBufferStartPosition < position
                //{15, 2, 2, 0L, 2, MOCK, mockFileChannel(5), false, false}, // writeBufferStartPosition > position
                //{15, 2, 2, 2L, 2, MOCK, mockFileChannel(5), false, false}, // writeBufferStartPosition = position
                //{15, 2, 2, 5L, 2, MOCK, mockFileChannel(-3), false, false},
                //{15, 2, 2, 5L, 2, MOCK, mockFileChannel(0), false, false},
                ////numReadBytes < numWriteBytes
                //{15, 5, 1, 0L, 5, WRITE, returnFileChannel(), false, IOException.class},
                //{15, 5, 0, 0L, 5, WRITE, returnFileChannel(), false, IOException.class},
                ////numReadBytes > numWriteBytes && length >= numWriteBytes
                //{15, 5, 10, 0L, 10, WRITE, returnFileChannel(), false, IOException.class},
                ////numReadBytes > numWriteBytes && length == numWriteBytes
                //{15, 5, 8, 0L, 5, WRITE, returnFileChannel(), false, false},
                ////numReadBytes = numWriteBytes = length
                //{15, 5, 5, 0L, 5, WRITE, returnFileChannel(), false, false},
                ////numWriteBytes - position <=  numReadBytes && numWriteBytes - position >= length
                //{12, 8, 5, 5L, 1, WRITE, returnFileChannel(), false, false},
                //{15, 5, 8, 0L, 4, WRITE, returnFileChannel(), false, false},
                //{15, 5, 4, 1L, 4, WRITE, returnFileChannel(), false, false},
                ////numWriteBytes - position >  numReadBytes && numReadBytes >= length
                //{15, 8, 4, 0L, 2, WRITE, returnFileChannel(), false, false},
                //{15, 8, 4, 1L, 4, WRITE, returnFileChannel(), false, false},
                ////numWriteBytes - position <=  numReadBytes && numReadBytes < length
                //{15, 8, 10, 0L, 12, WRITE, returnFileChannel(), false, IOException.class},
                //{15, 8, 8, 0L, 12, WRITE, returnFileChannel(), false, IOException.class},
                ////numWriteBytes - position >  numReadBytes && numReadBytes < length
                //{15, 10, 8, 0L, 10, WRITE, returnFileChannel(), false, IOException.class},
                //// line coverage 250 PIT
                //{255, 256, 256, 0L, 256, WRITE, returnFileChannel(), false, false},
        });
    }

    @Test
    public void testRead() {
        Object error = false;
        int expectedNumBytes = 0;
        try {
            ByteBuf readByteBuf = null;
            if (!isNotNullByteBuff) {
                readByteBuf = Unpooled.buffer(this.numReadBytes, this.numReadBytes);
            }

            int actualNumBytes = this.bufferedChannel.read(readByteBuf, this.position, this.length);

            int d = (int) (this.numWriteBytes - this.position);
            if ((d <= this.numReadBytes && d >= this.length) || (d > this.numReadBytes && this.numReadBytes >= this.length)) {
                if (this.length > 0) {
                    expectedNumBytes = Math.min(d, this.numReadBytes);
                }
            }
            Assert.assertEquals(expectedNumBytes, actualNumBytes);
        } catch (IllegalArgumentException | IOException | NullPointerException e) {
            error = e.getClass();
        }
        Assert.assertEquals(isExceptionExpected, error);
    }

    private ByteBuf createByteBuff(int numWriteBytes) {
        ByteBuf writeBuf = Unpooled.buffer(numWriteBytes, numWriteBytes);
        byte[] data = new byte[numWriteBytes];
        Random random = new Random();
        random.nextBytes(data);
        writeBuf.writeBytes(data);
        return writeBuf;
    }

    private static File createTempFile() throws IOException {
        File tempFile = File.createTempFile("file", "log");
        tempFile.deleteOnExit();
        return tempFile;
    }

    private static FileChannel returnFileChannel() throws IOException {
        File newFile = createTempFile();
        return new RandomAccessFile(newFile, "rw").getChannel();
    }

    private static FileChannel mockFileChannel(int readBytes) throws IOException {
        FileChannel fileChannel = mock(FileChannel.class);
        Mockito.when(fileChannel.position()).thenReturn(2L);
        Mockito.when(fileChannel.read(any(ByteBuffer.class), anyLong())).thenReturn(readBytes);
        return fileChannel;
    }


    private ByteBufAllocator mockByteBufAllocator() {
        ByteBufAllocator byteBufAllocator = mock(ByteBufAllocator.class);
        Mockito.when(byteBufAllocator.directBuffer()).thenReturn(null);
        return byteBufAllocator;
    }

    public enum WriteBeforeReadingType {
        WRITE,
        NOT_WRITE,
        MOCK
    }

}
