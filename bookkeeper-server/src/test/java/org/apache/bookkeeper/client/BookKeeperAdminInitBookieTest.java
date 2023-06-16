package org.apache.bookkeeper.client;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.test.ZooKeeperClusterUtil;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class BookKeeperAdminInitBookieTest {

    private ServerConfiguration conf; // {null, new ServerConfiguration(), invalid}
    private final String bookieID; // {null, empty, notEmpty}
    private final boolean addJournal; // {true, false}
    private final boolean addLedger; // {true, false}
    private final boolean addIndexConf; // {true, false}
    private final boolean addIndex; // {true, false}
    private final Object expected;

    private ZooKeeperCluster zk;

    public BookKeeperAdminInitBookieTest(ServerConfiguration conf, String bookieID, boolean addJournal, boolean getLedger, boolean addIndexConf, boolean addIndex, Object expected) {
        this.conf = conf;
        this.bookieID = bookieID;
        this.addJournal = addJournal;
        this.addLedger = getLedger;
        this.addIndexConf = addIndexConf;
        this.addIndex = addIndex;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection<?> getParameters() {
        return Arrays.asList(new Object[][]{
                {null, "123", false, false, false, true, NullPointerException.class},
                {new ServerConfiguration(), "123", false, true, false, true, false},
                {new ServerConfiguration(), null, true, true, false, true, false},
                {new ServerConfiguration(), "1234", true, true, false, true, false},
                {new ServerConfiguration(), "", true, true, true, true, IllegalArgumentException.class},
                {new ServerConfiguration(), "564", false, false, false, true, true},
                // line coverage 1370 PIT
                {new ServerConfiguration(), "123", false, true, false, true, false},
                // line coverage 1376 PIT
                {new ServerConfiguration(), "123", false, false, true, true, false},
                // line coverage 1374 JACOCO
                {new ServerConfiguration(), "123", false, false, false, false, true},
                // line coverage 1375 JACOCO
                {new ServerConfiguration(), "123", false, false, true, false, true},
                {new ServerConfiguration(), "123", false, false, true, true, false},

        });
    }

    @Test
    public void testInitBookie() throws Exception {
        Object result;

        try {
            if (this.bookieID != null) {
                this.conf.setBookieId(this.bookieID);
            }
            if (this.addJournal) {
                addFileDir(this.conf.getJournalDirs());
            }
            if (this.addLedger) {
                addFileDir(this.conf.getLedgerDirs());
            }
            if (this.addIndexConf) {
                this.conf.setIndexDirName(new String[]{"/testIndex/fill.txt"});
                if (this.addIndex)
                    addFileDir(this.conf.getIndexDirs());
            }

            mockRegistrationManager(this.conf);
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
        if (this.addJournal && FileUtils.getFile("/tmp/bk-txn").exists())
            FileUtils.cleanDirectory(FileUtils.getFile("/tmp/bk-txn"));
        if (this.addLedger && FileUtils.getFile("/tmp/bk-data").exists())
            FileUtils.cleanDirectory(FileUtils.getFile("/tmp/bk-data"));
        if (this.addIndexConf && FileUtils.getFile("/testIndex/fill.txt").exists())
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

    /**
     * Mock RegistrationManager
     *
     * @param conf:
     * @throws UnknownHostException:
     * @throws BookieException:
     */
    private void mockRegistrationManager(ServerConfiguration conf) throws UnknownHostException, BookieException {
        BookieId bookieId = BookieImpl.getBookieId(conf);
        MetadataBookieDriver metadataBookieDriver = mock(MetadataBookieDriver.class);
        RegistrationManager registrationManager = mock(RegistrationManager.class);

        Mockito.when(metadataBookieDriver.createRegistrationManager()).thenReturn(registrationManager);
        Mockito.when(registrationManager.isBookieRegistered(bookieId)).thenReturn(false);
        Mockito.when(registrationManager.readCookie(bookieId)).thenThrow(new BookieException.CookieNotFoundException());
    }
}