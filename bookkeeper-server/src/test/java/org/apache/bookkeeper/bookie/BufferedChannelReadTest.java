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
    private static final int NUM_BYTES = 80;
    private static final long WRITE_BUFFER_START_POSITION = 2L;
    private BufferedChannel bufferedChannel;
    private ByteBuf dest;
    private final long position; // {<capacità write buffer}, {>capacità write buffer}, {=capacità write buffer}
    private final int length; // {< capacità write buffer - pos}, {< capacità write buffer - pos}, {= capacità write buffer - pos}
    private final Object isExceptionExpected;

    public BufferedChannelReadTest(Buffer destBuffer, long position, int length, FileChannelType fileChannel, Object isExceptionExpected) throws IOException {
        this.position = position;
        this.length = length;
        this.isExceptionExpected = isExceptionExpected;
        configure(destBuffer, fileChannel);
    }

    private void configure(Buffer bufferType, FileChannelType fileChannelType) throws IOException {
        FileChannel fileChannel = null;
        switch (fileChannelType) {
            case FILE_CHANNEL:
                fileChannel = returnFileChannel();
                break;
            case MOCK_FILE_CHANNEL:
                fileChannel = mockFileChannel();
                break;
        }

        this.bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel, CAPACITY);
        this.bufferedChannel.write(createByteBuff());
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
            case MOCK_BUFFER:
                this.bufferedChannel = new BufferedChannel(mockByteBufAllocator(), fileChannel, CAPACITY);
                break;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //dest, numWriteBytes, numReadBytes, pos (read), length (read)
                //ByteBuff NULL
                {Buffer.NULL, 0L, 2, FileChannelType.FILE_CHANNEL, NullPointerException.class},
                //ByteBuff EMPTY
                {Buffer.EMPTY, 0L, 0, FileChannelType.FILE_CHANNEL, 0},
                //pos < NUM_BYTES && length < NUM_BYTES - pos
                {Buffer.VALID, NUM_BYTES - 10, 5, FileChannelType.FILE_CHANNEL, 10},
                //pos < NUM_BYTES && length > NUM_BYTES - pos
                {Buffer.VALID, NUM_BYTES - 1, NUM_BYTES + 1, FileChannelType.FILE_CHANNEL, IOException.class},
                //pos < NUM_BYTES && length < NUM_BYTES && length = NUM_BYTES - pos
                {Buffer.VALID, NUM_BYTES - 1, 1, FileChannelType.FILE_CHANNEL, 1},
                //pos > NUM_BYTES
                {Buffer.VALID, NUM_BYTES + 1, 10, FileChannelType.FILE_CHANNEL, IllegalArgumentException.class},
                //pos = NUM_BYTES && length == 0
                {Buffer.VALID, NUM_BYTES, 0, FileChannelType.FILE_CHANNEL, 0},

                {Buffer.VALID, -NUM_BYTES, 10, FileChannelType.FILE_CHANNEL, IllegalArgumentException.class},  // writeBufferStartPosition > position
                {Buffer.VALID, 0L, 10, FileChannelType.FILE_CHANNEL, 80}, // writeBufferStartPosition = position

                // MOCK FILE CHANNEL
                {Buffer.VALID, WRITE_BUFFER_START_POSITION + 1, 1, FileChannelType.MOCK_FILE_CHANNEL, 79}, // writeBufferStartPosition < position
                {Buffer.VALID, WRITE_BUFFER_START_POSITION - 1, 1, FileChannelType.MOCK_FILE_CHANNEL, 80}, // writeBufferStartPosition > position
                {Buffer.VALID, WRITE_BUFFER_START_POSITION, 1, FileChannelType.MOCK_FILE_CHANNEL, 80}, // writeBufferStartPosition = position

                // MOCK FILE CHANNEL && MOCK WRITE BUFFER == NULL
                {Buffer.MOCK_BUFFER, WRITE_BUFFER_START_POSITION + 1, 1, FileChannelType.MOCK_FILE_CHANNEL, 0}, // writeBufferStartPosition < position
                {Buffer.MOCK_BUFFER, WRITE_BUFFER_START_POSITION - 1, 1, FileChannelType.MOCK_FILE_CHANNEL, NullPointerException.class}, // writeBufferStartPosition > position
                {Buffer.MOCK_BUFFER, WRITE_BUFFER_START_POSITION, 1, FileChannelType.MOCK_FILE_CHANNEL, 0}, // writeBufferStartPosition = position

        });
    }

    @Test
    public void testRead() {
        Object actual;

        try {
            actual = this.bufferedChannel.read(this.dest, this.position, this.length);
            System.out.println("Actual num bytes: " + actual);

        } catch (IllegalArgumentException | IOException | NullPointerException e) {
            actual = e.getClass();
            e.printStackTrace();
        }
        Assert.assertEquals(isExceptionExpected, actual);
    }

    /**
     * Creation of a ByteBuf
     *
     * @return: ByteBuf
     */
    private ByteBuf createByteBuff() {
        ByteBuf writeBuf = Unpooled.buffer(NUM_BYTES, NUM_BYTES);
        byte[] data = new byte[BufferedChannelReadTest.NUM_BYTES];
        Random random = new Random();
        random.nextBytes(data);
        writeBuf.writeBytes(data);
        return writeBuf;
    }

    /**
     * Creation of a temporary file
     *
     * @throws IOException:
     * @return: File
     */
    private static File createTempFile() throws IOException {
        File tempFile = File.createTempFile("file", "log");
        tempFile.deleteOnExit();
        return tempFile;
    }

    /**
     * @throws IOException:
     * @return: FileChannel
     */
    private static FileChannel returnFileChannel() throws IOException {
        File newFile = createTempFile();
        return new RandomAccessFile(newFile, "rw").getChannel();
    }

    /**
     * Mock FileChannel
     *
     * @throws IOException:
     * @return: FileChannel
     */
    private static FileChannel mockFileChannel() throws IOException {
        FileChannel fileChannel = mock(FileChannel.class);
        Mockito.when(fileChannel.position()).thenReturn(WRITE_BUFFER_START_POSITION);
        Mockito.when(fileChannel.read(any(ByteBuffer.class), anyLong())).thenReturn(NUM_BYTES);
        return fileChannel;
    }

    /**
     * Mock ByteBufAllocator
     *
     * @return: ByteBufAllocator
     */
    private ByteBufAllocator mockByteBufAllocator() {
        ByteBufAllocator byteBufAllocator = mock(ByteBufAllocator.class);
        Mockito.when(byteBufAllocator.directBuffer()).thenReturn(null);
        return byteBufAllocator;
    }

    private enum FileChannelType {
        FILE_CHANNEL,
        MOCK_FILE_CHANNEL
    }

    private enum Buffer {
        VALID,
        EMPTY,
        NULL,
        MOCK_BUFFER
    }

}
