package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.test.ZooKeeperClusterUtil;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BookKeeperAdminInitBookieTest {

    private ServerConfiguration conf; // {null, new ServerConfiguration(), invalid}
    private final String bookieID; // {null, empty, notEmpty"
    private final boolean getJournal; // {true, false}
    private final boolean getLedger; // {true, false}
    private final boolean getIndex; // {true, false}
    private final Object expected;

    private ZooKeeperCluster zk;

    public BookKeeperAdminInitBookieTest(ServerConfiguration conf, String bookieID, boolean getJournal, boolean getLedger, boolean getIndex, Object expected) {
        this.conf = conf;
        this.bookieID = bookieID;
        this.getJournal = getJournal;
        this.getLedger = getLedger;
        this.getIndex = getIndex;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection<?> getParameters() {
        return Arrays.asList(new Object[][]{
                {null, "123", false, false, false, NullPointerException.class},
                {new ServerConfiguration(), "123", false, true, false, false},
                {new ServerConfiguration(), null, true, true, false, false},
                {new ServerConfiguration(), "1234", true, true, false, false},
                {new ServerConfiguration(), "", true, true, true, IllegalArgumentException.class},
                {new ServerConfiguration(), "564", false, false, false, true},
                // line coverage 1370 PIT
                {new ServerConfiguration(), "123", false, true, false, false},
                // line coverage 1376 PIT
                //{new ServerConfiguration(), "123", false, false, true, false},

        });
    }

    @Test
    public void testInitBookie() throws Exception {
        Object result;

        try {
            if (this.bookieID != null) {
                this.conf.setBookieId(this.bookieID);
            }

            if (this.getJournal) {
                addFileDir(this.conf.getJournalDirs());
            }
            if (this.getLedger) {
                addFileDir(this.conf.getLedgerDirs());
            }
            if (this.getIndex) {
                this.conf.setIndexDirName(new String[]{"/testIndex/fill.txt"});
                addFileDir(this.conf.getIndexDirs());
            }

            result = BookKeeperAdmin.initBookie(this.conf);
        } catch (NullPointerException | IllegalArgumentException e) {
            result = e.getClass();
        }
        Assert.assertEquals(this.expected, result);
    }

    @Before
    public void setUp() {
        try {
            zk = new ZooKeeperClusterUtil(3);
            zk.startCluster();
            //E' necessario specificare lo URI dove vengono salvati i ledgers
            if (this.conf != null) {
                this.conf = TestBKConfiguration.newServerConfiguration();
                this.conf.setMetadataServiceUri(zk.getMetadataServiceUri());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @After
    public void tearDown() throws IOException {
        if (this.getJournal && FileUtils.getFile("/tmp/bk-txn").exists())
            FileUtils.cleanDirectory(FileUtils.getFile("/tmp/bk-txn"));
        if (this.getLedger && FileUtils.getFile("/tmp/bk-data").exists())
            FileUtils.cleanDirectory(FileUtils.getFile("/tmp/bk-data"));
        if (this.getIndex && FileUtils.getFile("/testIndex/fill.txt").exists())
            FileUtils.cleanDirectory(FileUtils.getFile("/testIndex/fill.txt"));

        deleteDirectory();
        try {
            zk.getZooKeeperClient().close();
            zk.killCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addFileDir(File[] dirs) throws IOException {
        for (File dir : dirs) {
            dir.setWritable(true);
            dir.setExecutable(true);
            dir.mkdirs();
            //file name only
            File file = new File(dir.getAbsolutePath() + "/prova.txt");
            System.out.println(file.canWrite());
            if (file.createNewFile()) {
                System.out.println("file.txt File Created in Project root directory");
            } else System.out.println("File file.txt already exists in the project root directory");

        }
    }

    public void deleteDirectory() throws IOException {
        if (this.conf != null) {
            for (File dir : this.conf.getJournalDirs()) {
                FileUtils.deleteDirectory(dir);
            }
            for (File dir : this.conf.getLedgerDirs()) {
                FileUtils.deleteDirectory(dir);
            }
        }
    }
}