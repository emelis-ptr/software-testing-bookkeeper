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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

@RunWith(value = Parameterized.class)
public class BufferedChannelReadTest {

    private static final int CAPACITY = 100;
    private static int NUM_BYTES = 80;
    private BufferedChannel bufferedChannel;
    private FileChannel fileChannel;
    private ByteBuf dest;
    private final long position; // {<0, =>0}
    private final int length; // {<0, =>0}
    private final Object isExceptionExpected;

    public BufferedChannelReadTest(Buffer destBuffer, long position, int length, FileChannelType fileChannel, Object isExceptionExpected) throws IOException {
        this.position = position;
        this.length = length;
        this.isExceptionExpected = isExceptionExpected;
        configure(destBuffer, fileChannel);
    }

    private void configure(Buffer bufferType, FileChannelType fileChannelType) throws IOException {
        switch (fileChannelType) {
            case FILE_CHANNEL:
                this.fileChannel = returnFileChannel();
            case MOCK_FILE_CHANNEL:
                this.fileChannel = mockFileChannel(NUM_BYTES);
        }

        this.bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel, CAPACITY);
        this.bufferedChannel.write(createByteBuff(NUM_BYTES));

        switch (bufferType) {
            case VALID: {
                this.dest = Unpooled.buffer(NUM_BYTES, NUM_BYTES);
                break;
            }
            case EMPTY: {
                this.dest = Unpooled.buffer(0, 0);
                break;
            }
            case NULL: {
                this.dest = null;
                break;
            }

        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() throws IOException {
        return Arrays.asList(new Object[][]{
                //dest, numWriteBytes, numReadBytes, pos (read), length (read)
                //ByteBuff NULL
                {Buffer.NULL, 0L, 2, FileChannelType.FILE_CHANNEL, NullPointerException.class},
                //ByteBuff EMPTY
                {Buffer.EMPTY, 0L, 0, FileChannelType.FILE_CHANNEL, 0},
                //pos < NUM_BYTES && length < NUM_BYTES - pos
                {Buffer.VALID, NUM_BYTES - 10, 5, FileChannelType.FILE_CHANNEL, 12},
                //pos < NUM_BYTES && length > NUM_BYTES - pos
                {Buffer.VALID, NUM_BYTES - 1, NUM_BYTES + 1, FileChannelType.FILE_CHANNEL, IOException.class},
                //pos < NUM_BYTES && length < NUM_BYTES && length = NUM_BYTES - pos
                {Buffer.VALID, NUM_BYTES - 1, 1, FileChannelType.FILE_CHANNEL, 3},
                //pos > NUM_BYTES
                {Buffer.VALID, NUM_BYTES + 1, 10, FileChannelType.FILE_CHANNEL, IOException.class},
                //pos = NUM_BYTES && length == 0
                {Buffer.VALID, NUM_BYTES, 0, FileChannelType.FILE_CHANNEL, 0},

                {Buffer.VALID, -NUM_BYTES, 10, FileChannelType.FILE_CHANNEL, 80},  // writeBufferStartPosition > position
                {Buffer.VALID, 0L, 10, FileChannelType.FILE_CHANNEL, 80}, // writeBufferStartPosition = position

                // MOCK FILE CHANNEL
                {Buffer.VALID, NUM_BYTES + 1, 1, FileChannelType.MOCK_FILE_CHANNEL, 1}, // writeBufferStartPosition < position
                {Buffer.VALID, NUM_BYTES - 1, 1, FileChannelType.MOCK_FILE_CHANNEL, 3}, // writeBufferStartPosition > position
                {Buffer.VALID, NUM_BYTES, 1, FileChannelType.MOCK_FILE_CHANNEL, 2}, // writeBufferStartPosition = position

                /*// not writing before reading
                {2, 2, 0L, 2, NOT_WRITE, returnFileChannel(), false, IOException.class},
                {2, 2, 0L, 2, NOT_WRITE, mockFileChannel(5), false, false},
                {2, 2, 5L, 1, WRITE, returnFileChannel(), false, IllegalArgumentException.class}, // writeBufferStartPosition < position
                {2, 2, -1L, 1, WRITE, returnFileChannel(), false, IllegalArgumentException.class}, // writeBufferStartPosition > position
                {2, 2, 0L, 2, WRITE, returnFileChannel(), false, false},// writeBufferStartPosition = position
                // writeBuffer = null
                {2, 2, 5L, 2, MOCK, mockFileChannel(5), false, false}, // writeBufferStartPosition < position
                {2, 2, 0L, 2, MOCK, mockFileChannel(5), false, false}, // writeBufferStartPosition > position
                {2, 2, 2L, 2, MOCK, mockFileChannel(5), false, false}, // writeBufferStartPosition = position
                {2, 2, 5L, 2, MOCK, mockFileChannel(-3), false, false},
                {2, 2, 5L, 2, MOCK, mockFileChannel(0), false, false},*/

                // line coverage 250
                //{Buffer.VALID, 256, 256, 0L, 256, WRITE, returnFileChannel(), false, false},
        });
    }

    @Test
    public void testRead() {
        Object actual = 0;
        int expectedNumBytes = 0;

        try {
            actual = this.bufferedChannel.read(this.dest, this.position, this.length);
            System.out.println("Actual num bytes: " + actual);

            /*int d = (int) (NUM_WRITE - this.position);
            if ((d <= NUM_READ && d >= this.length) || (d > NUM_READ && NUM_READ >= this.length)) {
                if (this.length > 0) {
                    expectedNumBytes = Math.min(d, NUM_READ);
                }
            }
            System.out.println("Expected num bytes: " + expectedNumBytes);*/
            //Assert.assertEquals(expectedNumBytes, actualNumBytes);
        } catch (IllegalArgumentException | IOException | NullPointerException e) {
            actual = e.getClass();
        }
        Assert.assertEquals(isExceptionExpected, actual);
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

    private enum FileChannelType {
        FILE_CHANNEL,
        MOCK_FILE_CHANNEL
    }

    private enum Buffer {
        VALID,
        EMPTY,
        NULL
    }

}
