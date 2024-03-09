package simpledb.transaction;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import simpledb.common.Database;

import java.io.*;

/**
 * Transaction encapsulates information about the state of
 * a transaction and manages transaction commit / abort.
 */

public class Transaction {

    // final static Logger logger = LoggerFactory.getLogger(Transaction.class);
    private final TransactionId tid;
    volatile boolean started = false;

    public Transaction() {
        tid = new TransactionId();
    }

    /** Start the transaction running */
    public void start() {
        started = true;
        try {
            Database.getLogFile().logXactionBegin(tid);
        } catch (IOException e) {
            // logger.error(e.getMessage());
        }
    }

    public TransactionId getId() {
        return tid;
    }

    /** Finish the transaction */
    public void commit() throws IOException {
        transactionComplete(false);
    }

    /** Finish the transaction */
    public void abort() throws IOException {
        transactionComplete(true);
    }

    /** Handle the details of transaction commit / abort */
    public void transactionComplete(boolean abort) throws IOException {

        if (started) {
            //write abort log record and rollback transaction
            if (abort) {
                Database.getLogFile().logAbort(tid); //does rollback too
            } 

            // Release locks and flush pages if needed
            Database.getBufferPool().transactionComplete(tid, !abort); // release locks

            // write commit log record
            if (!abort) {
            	Database.getLogFile().logCommit(tid);
            }

            //setting this here means we could possibly write multiple abort records -- OK?
            started = false;
        }
    }
}
