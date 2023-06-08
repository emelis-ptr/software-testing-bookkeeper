package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value = Parameterized.class)
public class BufferedChannelWriteTest {
    private static final String dir = "test";
    private static final String fileName = "writeFile";
    private final ByteBuf writeByteBuf; // {null, empty, notEmpty, invalid}
    private final int bufferedCapacity; //{<0, =0, >0}
    private final long unpersistedBytesBound;
    private final boolean shouldForceWrite;
    private final Object expected;
    private BufferedChannel bufferedChannel;
    private FileChannel fileChannel;

    public BufferedChannelWriteTest(ByteBuf writeByteBuf, int bufferedCapacity, long unpersistedBytesBound, boolean shouldForceWrite, Object expected) {
        this.writeByteBuf = writeByteBuf;
        this.bufferedCapacity = bufferedCapacity;
        this.unpersistedBytesBound = unpersistedBytesBound;
        this.shouldForceWrite = shouldForceWrite;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection getParameters() {

        return Arrays.asList(new Object[][]{
                //writeByteBuf, bufferedCapacity, unpersistedBytesBound, expected
                {null, 0, 0L, false, NullPointerException.class},
                {createByteBuf(2), -1, 2L, false, IllegalArgumentException.class}, //capacity < 0
                {createByteBuf(2), 1, 2L, false, 0}, // capacity < byteBuff length
                {createByteBuf(2), 1, 0L, false, 0},
                {createByteBuf(5), 0, -1L, false, 0}, //loop with capacity = 0
                {createByteBuf(0), 2, 2L, false, 0}, // capacity > byteBuff length
                {createByteBuf(1), 2, 2L, false, 1},
                {createByteBuf(2), 2, 0L, false, 0}, // capacity = byteBuff length
                {createByteBuf(0), 0, -2L, false, 0}, // byteBuff empty
                // line coverage 123 & 134
                {createByteBuf(15), 10, 0L, false, 5},
                // line coverage 136
                {createByteBuf(5), 10, 5L, true, 0},
                {mockByteBuf(0, 0), 2, 2L, false, 0}, // invalid byteBuff
                {mockByteBuf(1, -1), 2, 0L, false, IndexOutOfBoundsException.class},
        });
    }


    @Test
    public void testWrite() {
        Object result;

        try {
            if (this.writeByteBuf != null) {
                bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fileChannel, this.bufferedCapacity, this.unpersistedBytesBound);
                System.out.println(this.bufferedChannel.unpersistedBytes.get());
                if (this.bufferedCapacity != 0) {
                    this.bufferedChannel.write(this.writeByteBuf);
                }
            }
            System.out.println(this.bufferedChannel.unpersistedBytes.get());
            result = this.bufferedChannel.getNumOfBytesInWriteBuffer();
        } catch (NullPointerException | IllegalArgumentException | IOException | IndexOutOfBoundsException e) {
            result = e.getClass();
        }
        Assert.assertEquals("Error expected", this.expected, result);
    }

    @BeforeClass
    public static void setupEnv() {
        //se non esiste, la cartella viene creata
        if (!Files.exists(Paths.get(dir))) {
            File tmpDir = new File(dir);
            tmpDir.mkdir();
        }
    }

    @Before
    public void setUp() throws IOException {
        File newFile = File.createTempFile(fileName, "log", new File(dir));
        newFile.deleteOnExit();

        RandomAccessFile randomAccessFile = new RandomAccessFile(newFile, "rw");
        this.fileChannel = randomAccessFile.getChannel();
        this.fileChannel.position(this.fileChannel.size());
    }

    @After
    public void tearDown() throws IOException {
        this.fileChannel.close();
        if (bufferedChannel != null) this.bufferedChannel.close();
    }

    private static ByteBuf createByteBuf(int length) {
        ByteBuf byteBuf = Unpooled.buffer(length);

        if (length == 0) {
            return byteBuf;
        }
        byte[] data = new byte[length];
        Random random = new Random();
        random.nextBytes(data); // Si riempie il buffer con dati random
        byteBuf.writeBytes(data);
        return byteBuf;
    }

    private static ByteBuf mockByteBuf(int readableBytes, int readerIndex) {
        ByteBuf byteBuf = mock(ByteBuf.class);
        when(byteBuf.readableBytes()).thenReturn(readableBytes);
        when(byteBuf.readerIndex()).thenReturn(readerIndex);
        return byteBuf;
    }
}
