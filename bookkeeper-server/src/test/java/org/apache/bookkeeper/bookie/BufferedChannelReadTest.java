package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;

@RunWith(value = Parameterized.class)
public class BufferedChannelReadTest {
    private BufferedChannel bufferedChannel;
    private final long position; // {<0, =>0}
    private final int length; // {<0, =>0}
    private final int numWriteBytes;
    private final int numReadBytes;
    private final boolean isByteBuffNull;
    private final Object isExceptionExpected;

    public BufferedChannelReadTest(int capacity, int numberByteWritten, int numReadBytes, long position, int length, boolean writeBeforeReading, FileChannel fileChannel, boolean isByteBuffNull, Object isExceptionExpected) throws IOException {
        this.position = position;
        this.length = length;
        this.numWriteBytes = numberByteWritten;
        this.numReadBytes = numReadBytes;
        this.isByteBuffNull = isByteBuffNull;
        this.isExceptionExpected = isExceptionExpected;
        configure(capacity, numberByteWritten, writeBeforeReading, fileChannel);
    }

    private void configure(int capacity, int numWriteBytes, boolean writeBeforeReading, FileChannel fileChannel) throws IOException {
        this.bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel, capacity);
        if (writeBeforeReading)
            this.bufferedChannel.write(createByteBuff(numWriteBytes));

        /*int readBytes = fileChannel.read(this.bufferedChannel.readBuffer.internalNioBuffer(0, this.bufferedChannel.readCapacity),
                this.position);
        System.out.println(readBytes);*/
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() throws IOException {
        return Arrays.asList(new Object[][]{
                //capacity, numWriteBytes, numReadBytes, pos (read), length (read)
                //ByteBuff null
                {2048, 256, 0, 0L, 2, true, returnFileChannel(), true, false},
                //ByteBuff not null
                {2048, 256, 256, -20L, 257, true, returnFileChannel(), false, IllegalArgumentException.class},
                {2048, 256, 256, 20L, 257, true, returnFileChannel(), false, IOException.class},
                {2048, 256, 256, 0L, -2, true, returnFileChannel(), false, false},
                {2048, 256, 256, 0L, 257, true, returnFileChannel(), false, IOException.class},
                {2048, 256, 64, 0L, 256, true, returnFileChannel(), false, IOException.class}, //numReadBytes < numWriteBytes
                //{2048, 256, 0, 0L, 256, IOException.class},
                {2048, 256, 1024, 0L, 1024, true, returnFileChannel(), false, IOException.class}, //numReadBytes > numWriteBytes && length >= numWriteBytes
                {2048, 256, 512, 0L, 256, true, returnFileChannel(), false, false}, //numReadBytes > numWriteBytes && length == numWriteBytes
                {1024, 256, 256, 0L, 256, true, returnFileChannel(), false, false},
                {2048, 256, 0, 0L, 0, true, returnFileChannel(), false, false},
                {255, 256, 256, 0L, 256, true, returnFileChannel(), false, false},
                {255, 256, 400, 0L, 258, true, returnFileChannel(), false, IOException.class},
                //numWriteBytes - position <=  numReadBytes && numWriteBytes - position >= length
                {1024, 512, 700, 500, 6, true, returnFileChannel(), false, false},
                {2048, 256, 512, 0, 200, true, returnFileChannel(), false, false},
                //numWriteBytes - position >  numReadBytes && numReadBytes >= length
                {2048, 1024, 400, 0, 43, true, returnFileChannel(), false, false},
                {2048, 1024, 400, 0, 400, true, returnFileChannel(), false, false},
                //numWriteBytes - position <=  numReadBytes && numReadBytes < length
                {2048, 1024, 2400, 0, 2000, true, returnFileChannel(), false, IOException.class},
                {1024, 512, 700, 500, 13, true, returnFileChannel(), false, IOException.class},
                //numWriteBytes - position >  numReadBytes && numReadBytes < length
                {2048, 1024, 400, 0, 401, true, returnFileChannel(), false, IOException.class},
                //writeBeforeReading = false
                {2048, 256, 256, 0L, -2, false, returnFileChannel(), false, false},
                //Mock FileChannel
                {2048, 256, 256, 0L, -2, false, mockFileChannel(), false, false},

                {255, 256, 0, 0L, 256, true, returnFileChannel(), true, false},
                {255, 256, 400, 0L, 258, true, returnFileChannel(), true, false},
        });
    }

    @Test
    public void testRead() {
        Object error = false;
        int expectedNumBytes = 0;
        try {
            ByteBuf readByteBuf;
            int actualNumBytes = 0;
            if (!isByteBuffNull) {
                readByteBuf = Unpooled.buffer(this.numReadBytes, this.numReadBytes);
                actualNumBytes = this.bufferedChannel.read(readByteBuf, this.position, this.length);
            }

            int d = (int) (this.numWriteBytes - this.position);
            if ((d <= this.numReadBytes && d >= this.length) || (d > this.numReadBytes && this.numReadBytes >= this.length)) {
                if (this.length > 0) {
                    expectedNumBytes = Math.min(d, this.numReadBytes);
                }
            }
            Assert.assertEquals(expectedNumBytes, actualNumBytes);
        } catch (IllegalArgumentException | IOException e) {
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

    private static FileChannel mockFileChannel() throws IOException {
        FileChannel fileChannel = mock(FileChannel.class);
        Mockito.when(fileChannel.read(any(ByteBuffer.class), anyInt())).thenReturn(2);
        return fileChannel;
    }

}
