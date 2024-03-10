package simpledb.index;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.Field;
import simpledb.storage.IntField;

import java.io.*;
import java.util.Arrays;

/**
 * Each instance of BTreeHeaderPage stores data for one page of a BTreeFile and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see BTreeFile
 * @see BufferPool
 *
 */
public class BTreeHeaderPage extends AbstractBTreePage {

	// final static Logger logger = LoggerFactory.getLogger(BTreeHeaderPage.class);
	final static int INDEX_SIZE = Type.INT_TYPE.getLen();

	final int numSlots;

	private int nextPage; // next header page or 0
	private int prevPage; // previous header page or 0

	byte[] oldData;
	private final Byte oldDataLock= (byte) 0;

	/**
	 * Create a BTreeHeaderPage from a set of bytes of data read from disk.
	 * The format of a BTreeHeaderPage is two pointers to the next and previous
	 * header pages, followed by a set of bytes indicating which pages in the file
	 * are used or available
	 * @see BufferPool#getPageSize()
	 * 
	 */
	public BTreeHeaderPage(BTreePageId id, byte[] data) throws IOException {
		super(id);
		this.numSlots = getNumSlots();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// Read the next and prev pointers
		try {
			Field f = Type.INT_TYPE.parse(dis);
			this.nextPage = ((IntField) f).getValue();
		} catch (java.text.ParseException e) {
			// logger.error(e.getMessage());
		}

		try {
			Field f = Type.INT_TYPE.parse(dis);
			this.prevPage = ((IntField) f).getValue();
		} catch (java.text.ParseException e) {
			// logger.error(e.getMessage());
		}

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		for (int i=0; i<header.length; i++)
			header[i] = dis.readByte();

		dis.close();

		setBeforeImage();
	}

	/**
	 * Initially mark all slots in the header used.
	 */
	public void init() {
        Arrays.fill(header, (byte) 0xFF);
	}

	/**
	 * Computes the number of bytes in the header while saving room for pointers
	 */
	private static int getHeaderSize() {        
		// pointerBytes: nextPage and prevPage pointers
		int pointerBytes = 2 * INDEX_SIZE; 
		return BufferPool.getPageSize() - pointerBytes;
	}

	/**
	 * Computes the number of slots in the header
	 */
	public static int getNumSlots() {        
		return getHeaderSize() * 8;
	}

	/** Return a view of this page before it was modified
        -- used by recovery */
	public BTreeHeaderPage getBeforeImage(){
		try {
			byte[] oldDataRef;
			synchronized(oldDataLock)
			{
				oldDataRef = oldData;
			}
			return new BTreeHeaderPage(pid,oldDataRef);
		} catch (IOException e) {
			// logger.error(e.getMessage());
			//should never happen -- we parsed it OK before!
			System.exit(1);
		}
		return null;
	}

	public void setBeforeImage() {
		synchronized(oldDataLock)
		{
			oldData = getPageData().clone();
		}
	}

	/**
	 * @return the PageId associated with this page.
	 */
	public BTreePageId getId() {
		return pid;
	}

	/**
	 * Generates a byte array representing the contents of this page.
	 * Used to serialize this page to disk.
	 * <p>
	 * The invariant here is that it should be possible to pass the byte
	 * array generated by getPageData to the BTreeHeaderPage constructor and
	 * have it produce an identical BTreeHeaderPage object.
	 *
	 * @see #BTreeHeaderPage
	 * @return A byte array correspond to the bytes of this page.
	 */
	public byte[] getPageData() {
		int len = BufferPool.getPageSize();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// write out the next and prev pointers
		try {
			dos.writeInt(nextPage);
			dos.writeInt(prevPage);
		} catch (IOException e) {
			// logger.error(e.getMessage());
		}


		// create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                // logger.error(e.getMessage());
            }
        }

		try {
			dos.flush();
		} catch (IOException e) {
			// logger.error(e.getMessage());
		}

		return baos.toByteArray();
	}

	/**
	 * Static method to generate a byte array corresponding to an empty
	 * BTreeHeaderPage.
	 * Used to add new, empty pages to the file. Passing the results of
	 * this method to the BTreeHeaderPage constructor will create a BTreeHeaderPage with
	 * no valid data in it.
	 *
	 * @return The returned ByteArray.
	 */
	public static byte[] createEmptyPageData() {
		return new byte[BufferPool.getPageSize()]; //all 0
	}

	/**
	 * Get the page id of the previous header page
	 * @return the page id of the previous header page
	 */
	public BTreePageId getPrevPageId() {
		return prevPage == 0 ? null : new BTreePageId(pid.getTableId(), prevPage, BTreePageId.HEADER);
	}

	/**
	 * Get the page id of the next header page
	 * @return the page id of the next header page
	 */
	public BTreePageId getNextPageId() {
		return nextPage == 0 ? null : new BTreePageId(pid.getTableId(), nextPage, BTreePageId.HEADER);
	}

	/**
	 * Set the page id of the previous header page
	 * @param id - the page id of the previous header page
	 */
	public void setPrevPageId(BTreePageId id) throws DbException {
		if(id == null) {
			prevPage = 0;
		}
		else {
			if(id.getTableId() != pid.getTableId()) {
				throw new DbException("table id mismatch in setPrevPageId");
			}
			if(id.getPageCategory() != BTreePageId.HEADER) {
				throw new DbException("prevPage must be a header page");
			}
			prevPage = id.getPageNumber();
		}
	}

	/**
	 * Set the page id of the next header page
	 * @param id - the page id of the next header page
	 */
	public void setNextPageId(BTreePageId id) throws DbException {
		if(id == null) {
			nextPage = 0;
		}
		else {
			if(id.getTableId() != pid.getTableId()) {
				throw new DbException("table id mismatch in setNextPageId");
			}
			if(id.getPageCategory() != BTreePageId.HEADER) {
				throw new DbException("nextPage must be a header page");
			}
			nextPage = id.getPageNumber();
		}
	}

	/**
	 * get the index of the first empty slot
	 * @return the index of the first empty slot or -1 if none exists
	 */
	public int getEmptySlot() {
		for (int i=0; i<header.length; i++) {
            for (int j = 0; j < 8; j++) {
                if (!isSlotUsed(i * 8 + j)) {
                    return i * 8 + j;
                }
            }
        }
		return -1;
	}
}
