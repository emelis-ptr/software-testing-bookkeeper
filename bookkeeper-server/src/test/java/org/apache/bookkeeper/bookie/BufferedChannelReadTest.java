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
public class BufferedChannelReadTest {
    private static final String dir = "test";
    private static final String fileName = "readFile";

    private FileChannel fileChannel;
    private BufferedChannel bufferedChannel;
    private final ByteBuf readByteBuf;

    private final int bufferedCapacity;
    private final long position;
    private final int lenght;
    private final long unpersistedBytesBound;

    private final Object expected;

    public BufferedChannelReadTest(ByteBuf readByteBuf, int bufferedCapacity, long position, int lenght, long unpersistedBytesBound, Object expected) {
        this.readByteBuf = readByteBuf;
        this.bufferedCapacity = bufferedCapacity;
        this.position = position;
        this.lenght = lenght;
        this.unpersistedBytesBound = unpersistedBytesBound;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection getParameters() {

        return Arrays.asList(new Object[][]{
                //readByteBuf, bufferedCapacity, position, lenght, unpersistedBytesBound, expected
                {null, 0, 0, 0, 0L, NullPointerException.class},
                {Unpooled.buffer(1), 1, 0L, 0, 0L, 0},
                {Unpooled.buffer(1), -1, 1L, 2, 1L, IllegalArgumentException.class},
                {Unpooled.buffer(2), 2, 0L, 2, 0L, 2},
                {Unpooled.buffer(2), 2, -1L, 2, 0L, IllegalArgumentException.class},
                {Unpooled.buffer(2), 2, 1L, -2, 0L, IllegalArgumentException.class},

        });
    }

    @Test
    public void testRead() {
        Object result;
        try {
            if (readByteBuf != null) {
                this.bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fileChannel, this.bufferedCapacity, this.unpersistedBytesBound);
                this.bufferedChannel.write(createByteBuf(this.lenght, this.position));
            }
            result = this.bufferedChannel.read(this.readByteBuf, this.position, this.lenght);

        } catch (NullPointerException | IllegalArgumentException | IOException e) {
            result = e.getClass();
        }
        Assert.assertEquals("Error", this.expected, result);
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
    public void setup() throws IOException {
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

    private static ByteBuf createByteBuf(int length, long position) {
        Random rand = new Random();
        ByteBuf byteBuf = Unpooled.buffer(length);
        byte[] bytes;
        if (length > 0) {
            bytes = new byte[length];
        } else {
            bytes = new byte[Math.toIntExact(position)];
        }
        rand.nextBytes(bytes);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }
}
