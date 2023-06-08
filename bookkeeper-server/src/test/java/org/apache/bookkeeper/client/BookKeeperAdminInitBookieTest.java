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

    private ServerConfiguration conf;
    private final String bookieID;
    private final Object expected;

    private ZooKeeperCluster zk;

    public BookKeeperAdminInitBookieTest(ServerConfiguration conf, String bookieID, Object expected) {
        this.conf = conf;
        this.bookieID = bookieID;
        this.expected = expected;
    }

    /**
     * Parametri
     * - ServerConfiguration -> {null, new ServerConfiguration()}
     *
     * @return :
     */
    @Parameterized.Parameters
    public static Collection<?> getParameters() {
        return Arrays.asList(new Object[][]{
                {null, "123", NullPointerException.class},
                {new ServerConfiguration(), "123", false},
                {new ServerConfiguration(), null, false},
                {new ServerConfiguration(), "1234", false},
                {new ServerConfiguration(), "", true}
        });
    }

    @Test
    public void testInitBookie() throws Exception {
        Object result;

        try {
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
                this.conf.setMetadataServiceUri(zk.getMetadataServiceUri());
                if (this.bookieID != null) {
                    this.conf.setBookieId(this.bookieID);
                }

                this.conf = TestBKConfiguration.newServerConfiguration();
                addFileDir(this.conf.getJournalDirs());
                addFileDir(this.conf.getLedgerDirs());
                this.conf.setIndexDirName(new String[]{"/testIndex/fill.txt"});
                addFileDir(this.conf.getIndexDirs());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @After
    public void tearDown() throws IOException {
        String bk_txn = "/tmp/bk-txn";
        if (FileUtils.getFile(bk_txn).exists()) {
            FileUtils.cleanDirectory(FileUtils.getFile(bk_txn));
            FileUtils.cleanDirectory(FileUtils.getFile("/tmp/bk-data"));
        }
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