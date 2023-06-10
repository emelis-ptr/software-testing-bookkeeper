package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.commons.io.FileUtils;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value = Parameterized.class)
public class BufferedChannelReadTest {
    private static final String dir = "test";
    private static final String fileName = "readFile";
    private final ByteBuf readByteBuf; // {null, empty, notEmpty, invalid}
    private final int bufferedCapacity; //{<0, =0, >0}
    private final long position;
    private final int lenght;
    private final long unpersistedBytesBound;
    private final Object expected;
    private FileChannel fileChannel;
    private RandomAccessFile randomAccessFile;
    private BufferedChannel bufferedChannel;

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
                {Unpooled.buffer(2), 2, 1L, -2, 0L, IllegalArgumentException.class},
                {Unpooled.buffer(2), 2, 0L, 2, 0L, 2},
                //line coverage 250
                //length > capacity - position, length > readByteBuf, position = capacity
                {Unpooled.buffer(1), 1, 1L, 2, 1L, IOException.class},
                //length < capacity - position, length < readByteBuf, position < capacity
                {Unpooled.buffer(1), 1, 0L, 0, 0L, 0},
                //length = capacity - position, length = readByteBuf, position < capacity
                {Unpooled.buffer(2), 2, 0L, 2, 0L, 2},
                // line coverage 251
                {Unpooled.buffer(10), 2, 4L, 2, 0L, IllegalArgumentException.class},
                // line coverage 264
                {Unpooled.buffer(2), 1, 0L, 2, 0L, 2},
                {Unpooled.buffer(0), 2, 0L, 2, 0L, 0}, // capacity = 0 -> loop
                {Unpooled.directBuffer(), 1, 0L, 2, 0L, 2},
                {mockByteBuf(), 1, 0L, 2, 0L, 0}, // mock readByteBuff

        });
    }

    @Test
    public void testRead() {
        Object result = 0;
        try {
            this.bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fileChannel, this.bufferedCapacity, this.unpersistedBytesBound);
            this.bufferedChannel.write(createByteBuf(this.lenght, this.position));

            if (this.readByteBuf.capacity() != 0) {
                result = this.bufferedChannel.read(this.readByteBuf, this.position, this.lenght);
            }

        } catch (NullPointerException | IllegalArgumentException |
                 IOException e) {
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

        randomAccessFile = new RandomAccessFile(newFile, "rw");
        this.fileChannel = randomAccessFile.getChannel();
        this.fileChannel.position(this.fileChannel.size());

    }

    @After
    public void tearDown() throws IOException {
        if (bufferedChannel != null) this.bufferedChannel.close();
        this.fileChannel.close();
        randomAccessFile.close();
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

    private static ByteBuf mockByteBuf() {
        ByteBuf byteBuf = mock(ByteBuf.class);
        when(byteBuf.writableBytes()).thenReturn(1);
        when(byteBuf.writeBytes(any(ByteBuf.class), any(int.class), any(int.class))).thenThrow(new IndexOutOfBoundsException());
        return byteBuf;
    }

    @AfterClass
    public static void cleanAll() throws IOException {
        FileUtils.cleanDirectory(FileUtils.getFile(dir));
    }
}
