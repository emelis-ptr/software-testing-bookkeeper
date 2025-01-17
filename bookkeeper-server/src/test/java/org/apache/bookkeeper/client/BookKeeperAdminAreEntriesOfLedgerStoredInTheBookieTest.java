package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class BookKeeperAdminAreEntriesOfLedgerStoredInTheBookieTest extends BookKeeperClusterTestCase {

    private final BookieId bookieAddress; //{null, new BookieSocketAddress}
    private final LedgerMetadata ledgerMetadata; // {null, LedgerMetadata}
    private final Object expected;

    private static final int NUM_OF_BOOKIES = 3;
    private static final long LEDGER_ID = 100L;

    public BookKeeperAdminAreEntriesOfLedgerStoredInTheBookieTest(BookieId bookieAddress, LedgerMetadata ledgerMetadata, Object expected) {
        super(NUM_OF_BOOKIES);
        this.bookieAddress = bookieAddress;
        this.ledgerMetadata = ledgerMetadata;
        this.expected = expected;
    }

    /**
     * Parametri:
     * ledgerId -> {<0L, 0L, >0L}
     * BookieId -> {null, new BookieSocketAddress()}
     * LedgerMetadata -> {null, new LedgerMetadataBuilder()}
     */
    @Parameterized.Parameters
    public static Collection<?> getParameter() throws Exception {
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        String PASSWORD = "testPasswd";

        int lastEntryId = 10;

        BookieId bookieAddress = new BookieSocketAddress("bookie0:3181").toBookieId();
        BookieId bookie1 = new BookieSocketAddress("bookie1:3181").toBookieId();
        BookieId bookie2 = new BookieSocketAddress("bookie2:3181").toBookieId();
        BookieId bookie3 = new BookieSocketAddress("bookie3:3181").toBookieId();

        List<BookieId> ensembleOfSegment1 = new ArrayList<>();
        ensembleOfSegment1.add(bookieAddress);
        ensembleOfSegment1.add(bookie1);
        ensembleOfSegment1.add(bookie2);

        List<BookieId> ensembleOfSegment2 = new ArrayList<>();
        ensembleOfSegment2.add(bookie3);
        ensembleOfSegment2.add(bookie1);
        ensembleOfSegment2.add(bookie2);

        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create()
                .withId(LEDGER_ID)
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withDigestType(digestType.toApiDigestType())
                .withPassword(PASSWORD.getBytes())
                .newEnsembleEntry(0, ensembleOfSegment1)
                .newEnsembleEntry(lastEntryId + 1, ensembleOfSegment2)
                .withLastEntryId(lastEntryId).withLength(65576)
                .withClosedState();

        LedgerMetadata ledgerMetadata = builder.build();

        return Arrays.asList(new Object[][]{
                {bookieAddress, ledgerMetadata, true},
                {bookieAddress, null, NullPointerException.class},
                {bookieAddress, ledgerMetadata, true},
                {bookieAddress, ledgerMetadata, true},
                {bookie1, ledgerMetadata, true},
                {null, ledgerMetadata, false},
                // line coverage 1686 PIT
                {bookie3, ledgerMetadata, false},
        });
    }

    @Test
    public void testAreEntriesOfLedgerStoredInTheBookie() {
        Object result;
        try {
            result = BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(LEDGER_ID, this.bookieAddress, this.ledgerMetadata);
        } catch (NullPointerException | IllegalArgumentException e) {
            result = e.getClass();
        }
        Assert.assertEquals(this.expected, result);
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

}
