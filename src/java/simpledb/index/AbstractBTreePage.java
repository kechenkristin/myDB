package simpledb.index;

import simpledb.common.Debug;
import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.function.Function;

public abstract class AbstractBTreePage implements Page {

    protected final BTreePageId pid;
    protected boolean dirty = false;
    protected TransactionId dirtier = null;

    protected byte[] header;

    public AbstractBTreePage(BTreePageId pid) {
        this.pid = pid;
    }

    /**
     * @return the PageId associated with this page.
     */
    @Override
    public PageId getId() {
        return pid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    @Override
    public TransactionId isDirty() {
        return dirty ? dirtier : null;
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    @Override
    public void markDirty(boolean dirty, TransactionId tid) {
        this.dirty = dirty;
        if (dirty) this.dirtier = tid;
    }

    Function<Integer, int[]> calHeader = i -> new int[]{i % 8, (i - i % 8) / 8};

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
//        int headerBit = i % 8;
//        int headerByte = (i - headerBit) / 8;
        int headerBit = calHeader.apply(i)[0];
        int headerByte = calHeader.apply(i)[1];
        return (header[headerByte] & (1 << headerBit)) != 0;
    }


    public void markSlotUsed(int i, boolean value) {
//        int headerBit = i % 8;
//        int headerByte = (i - headerBit) / 8;
        int headerBit = calHeader.apply(i)[0];
        int headerByte = calHeader.apply(i)[1];

        Debug.log(1, "BTreePage.setSlot: setting slot %d to %b", i, value);
        if (value)
            header[headerByte] |= (byte) (1 << headerBit);
        else
            header[headerByte] &= (byte) (0xFF ^ (1 << headerBit));
    }
}
