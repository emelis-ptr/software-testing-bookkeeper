package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

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

@RunWith(value = Parameterized.class)
public class BufferedChannelWriteTest {
    private static final String dir = "test";
    private static final String fileName = "writeFile";
    private final ByteBuf writeByteBuf; // {null, empty, notEmpty, invalid}
    private final int bufferedCapacity; //{<0, =0, >0}
    private final long unpersistedBytesBound;
    private final Object expected;
    private BufferedChannel bufferedChannel;
    private FileChannel fileChannel;
    private RandomAccessFile randomAccessFile;

    public BufferedChannelWriteTest(ByteBuf writeByteBuf, int bufferedCapacity, long unpersistedBytesBound, Object expected) {
        this.writeByteBuf = writeByteBuf;
        this.bufferedCapacity = bufferedCapacity;
        this.unpersistedBytesBound = unpersistedBytesBound;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection getParameters() {

        return Arrays.asList(new Object[][]{
                //writeByteBuf, bufferedCapacity, unpersistedBytesBound, expected
                {null, 0, 0L, NullPointerException.class},
                {createByteBuf(2), -1, 2L, IllegalArgumentException.class}, //capacity < 0
                {createByteBuf(2), 1, 2L, 0}, // capacity < byteBuff length
                {createByteBuf(2), 1, 2L, 0}, // capacity < byteBuff length
                {createByteBuf(20), 10, 2L, 0}, // capacity < byteBuff length
                {createByteBuf(2), 1, 0L, 0},
                {createByteBuf(20), 10, 0L, 0},
                {createByteBuf(5), 0, -1L, IOException.class}, //loop with capacity = 0
                {createByteBuf(0), 2, 2L, 0}, // capacity > byteBuff length
                {createByteBuf(0), 20, 2L, 0}, // capacity > byteBuff length
                {createByteBuf(0), 20, 0L, 0}, // capacity > byteBuff length
                {createByteBuf(1), 2, 2L, 1},
                {createByteBuf(10), 20, 2L, 0},
                {createByteBuf(2), 2, 0L, 0}, // capacity = byteBuff length
                {createByteBuf(20), 20, 1L, 0}, // capacity = byteBuff length
                {createByteBuf(0), 0, -2L, 0}, // byteBuff empty
                // line coverage 123 & 134 PIT
                {createByteBuf(15), 10, 0L, 5},
                // line coverage 136 PIT
                {createByteBuf(5), 10, 5L, 0},
                // Mock ByteBuff
                {mockByteBuf(), 10, 5L, 0},
        });
    }


    @Test
    public void testWrite() {
        Object result;

        try {

            int capacity = 0;
            if (this.writeByteBuf != null) {
                bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fileChannel, this.bufferedCapacity, this.unpersistedBytesBound);
                if (this.bufferedCapacity != 0) {
                    this.bufferedChannel.write(this.writeByteBuf);
                }
                capacity = this.writeByteBuf.capacity();
            }
            result = this.bufferedChannel.getNumOfBytesInWriteBuffer();

            ByteBuf buffer = Unpooled.buffer(capacity, capacity);
            int numBytesWritten = this.bufferedChannel.read(buffer, 0);
            Assert.assertEquals(capacity, numBytesWritten);
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

        randomAccessFile = new RandomAccessFile(newFile, "rw");
        this.fileChannel = randomAccessFile.getChannel();
        this.fileChannel.position(this.fileChannel.size());
    }

    @After
    public void tearDown() throws IOException {
        if (bufferedChannel != null) this.bufferedChannel.close();
        this.fileChannel.close();
        this.randomAccessFile.close();
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

    private static ByteBuf mockByteBuf() {
        ByteBuf byteBuf = mock(ByteBuf.class);
        Mockito.when(byteBuf.readableBytes()).thenReturn(0);
        return byteBuf;
    }

    @AfterAll
    public static void cleanAll() throws IOException {
        FileUtils.cleanDirectory(FileUtils.getFile(dir));
    }
}
