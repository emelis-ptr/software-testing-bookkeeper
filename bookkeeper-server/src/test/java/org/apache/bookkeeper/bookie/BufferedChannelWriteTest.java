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

@RunWith(value = Parameterized.class)
public class BufferedChannelWriteTest {
    private static final String dir = "test";
    private static final String fileName = "writeFile";
    private final ByteBuf writeByteBuf;
    private final int bufferedCapacity;
    private final long unpersistedBytesBound;
    private final Object expected;

    private BufferedChannel bufferedChannel;
    private FileChannel fileChannel;

    public BufferedChannelWriteTest(ByteBuf writeByteBuf, int bufferedCapacity, long unpersistedBytesBound, Object expected) {
        this.writeByteBuf = writeByteBuf;
        this.bufferedCapacity = bufferedCapacity;
        this.unpersistedBytesBound = unpersistedBytesBound;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection getParameters() {

        return Arrays.asList(new Object[][]{
                //writeByteBuf, bufferedCpacity, unpersistedBytesBound, expected
                {null, 0, 0L, NullPointerException.class},
                {createByteBuf(0), 2, 5L, 0},
                {createByteBuf(2), -1, 1L, IllegalArgumentException.class},
                {createByteBuf(1), 1, 0L, 0},
                {createByteBuf(2), 1, -1L, 0}
        });
    }


    @Test
    public void testWrite() throws IOException {
        Object result;

        try {
            if (this.writeByteBuf != null) {
                this.bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fileChannel, this.bufferedCapacity, this.unpersistedBytesBound);
                this.bufferedChannel.write(this.writeByteBuf);
            }
            result = this.bufferedChannel.getNumOfBytesInWriteBuffer();
        } catch (NullPointerException | IllegalArgumentException e) {
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
}
