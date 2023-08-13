package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.jupiter.api.AfterAll;
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

@RunWith(value = Parameterized.class)
public class BufferedChannelWriteTest {
    private static final String dir = "test";
    private static final String fileName = "writeFile";
    private ByteBuf writeByteBuf; // {null, empty, notEmpty}
    private final int bufferedCapacity; //{< capacity buffer, =capacity buffer, >capacity buffer}
    private final long unpersistedBytesBound;
    private final Object expected;

    private BufferedChannel bufferedChannel;
    private FileChannel fileChannel;
    private RandomAccessFile randomAccessFile;

    public BufferedChannelWriteTest(Buffer bufferType, int bufferedCapacity, long unpersistedBytesBound, Object expected) {

        switch (bufferType) {
            case VALID: {
                this.writeByteBuf = createByteBuf(50);
                break;
            }
            case EMPTY: {
                this.writeByteBuf = createByteBuf(0);
                break;
            }
            case NULL: {
                this.writeByteBuf = null;
                break;
            }

        }
        this.bufferedCapacity = bufferedCapacity;
        this.unpersistedBytesBound = unpersistedBytesBound;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection getParameters() {

        return Arrays.asList(new Object[][]{
                //writeByteBuf, bufferedCapacity, unpersistedBytesBound, expected
                {Buffer.NULL, 0, 0L, NullPointerException.class},
                {Buffer.VALID, 20, 0L, 10}, // capacity < byteBuff length &&  unpersistedBytesBound = OL
                {Buffer.VALID, 20, 2L, 0}, // capacity < byteBuff length &&  unpersistedBytesBound > OL
                {Buffer.VALID, 100, 0L, 50}, // capacity > byteBuff length &&  unpersistedBytesBound = OL
                {Buffer.VALID, 100, 2L, 0}, // capacity > byteBuff length &&  unpersistedBytesBound > OL
                {Buffer.VALID, 50, 0L, 0}, // capacity = byteBuff length &&  unpersistedBytesBound = OL
                {Buffer.VALID, 50, 2L, 0}, // capacity = byteBuff length &&  unpersistedBytesBound > OL
                {Buffer.EMPTY, 50, 0L, 0},
                // JACOCO LINE 136
                {Buffer.VALID, 100, 100L, 50},
                // PIT LINE 136
                {Buffer.VALID, 100, 50L, 0}
        });
    }


    @Test
    public void testWrite() {
        Object result;

        try {
            if (this.writeByteBuf != null) {
                bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fileChannel, this.bufferedCapacity, this.unpersistedBytesBound);
                this.bufferedChannel.write(this.writeByteBuf);
            }

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

    @AfterAll
    public static void cleanAll() throws IOException {
        FileUtils.cleanDirectory(FileUtils.getFile(dir));
    }

    private enum Buffer {
        VALID,
        EMPTY,
        NULL
    }
}
