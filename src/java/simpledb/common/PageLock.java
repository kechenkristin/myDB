package simpledb.common;

import simpledb.transaction.TransactionId;

public class PageLock {
    public static final int READ = 0;
    public static final int WRITE = 1;
    private int type;

    private TransactionId tid;

    public PageLock(int type, TransactionId transactionId) {
        tid = transactionId;
        this.type = type;
    }

    public static String lockTypeString(int acquireType) {
        return acquireType == 0 ? "Read lock" : "Write Type";
    }

    public int getType() {
        return this.type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
