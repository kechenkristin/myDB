package simpledb.index;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.TupleDesc;

/**
 * Each instance of BTreeInternalPage stores data for one page of a BTreeFile and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see BTreeFile
 * @see BufferPool
 * @author Kechen Liu
 */
public abstract class BTreePage extends AbstractBTreePage {
	protected final static int INDEX_SIZE = Type.INT_TYPE.getLen();

	protected final TupleDesc td;
	protected final int keyField;

	protected int parent; // parent is always internal node or 0 for root node
	protected final Byte oldDataLock= (byte) 0;


	/**
	 * Create a BTreeInternalPage from a set of bytes of data read from disk.
	 * The format of a BTreeInternalPage is a set of header bytes indicating
	 * the slots of the page that are in use, some number of entry slots, and extra
	 * bytes for the parent pointer, one extra child pointer (a node with m entries 
	 * has m+1 pointers to children), and the category of all child pages (either 
	 * leaf or internal).
	 *  Specifically, the number of entries is equal to: <p>
	 *          floor((BufferPool.getPageSize()*8 - extra bytes*8) / (entry size * 8 + 1))
	 * <p> where entry size is the size of entries in this index node
	 * (key + child pointer), which can be determined via the key field and 
	 * {@link Catalog#getTupleDesc}.
	 * The number of 8-bit header words is equal to:
	 * <p>
	 *      ceiling((no. entry slots + 1) / 8)
	 * <p>
	 * @see Database#getCatalog
	 * @see Catalog#getTupleDesc
	 * @see BufferPool#getPageSize()
	 * 
	 * @param id - the id of this page
	 * @param key - the field which the index is keyed on
	 */
	public BTreePage(BTreePageId id, int key) {
		super(id);
		this.keyField = key;
		this.td = Database.getCatalog().getTupleDesc(id.getTableId());
	}

	/**
	 * @return the PageId associated with this page.
	 */
	public BTreePageId getId() {
		return pid;
	}

	/**
	 * Static method to generate a byte array corresponding to an empty
	 * BTreePage.
	 * Used to add new, empty pages to the file. Passing the results of
	 * this method to the BTreeInternalPage or BTreeLeafPage constructor will create a BTreePage with
	 * no valid entries in it.
	 *
	 * @return The returned ByteArray.
	 */
	public static byte[] createEmptyPageData() {
		return new byte[BufferPool.getPageSize()]; //all 0
	}

	/**
	 * Get the parent id of this page
	 * @return the parent id
	 */
	public BTreePageId getParentId() {
		return parent == 0 ? BTreeRootPtrPage.getId(pid.getTableId()) : new BTreePageId(pid.getTableId(), parent, BTreePageId.INTERNAL);
	}

	/**
	 * Set the parent id
	 * @param id - the id of the parent of this page
	 * @throws DbException if the id is not valid
	 */
	public void setParentId(BTreePageId id) throws DbException {
		if(id == null) {
			throw new DbException("parent id must not be null");
		}
		if(id.getTableId() != pid.getTableId()) {
			throw new DbException("table id mismatch in setParentId");
		}
		if(id.getPageCategory() != BTreePageId.INTERNAL && id.getPageCategory() != BTreePageId.ROOT_PTR) {
			throw new DbException("parent must be an internal node or root pointer");
		}
		if(id.getPageCategory() == BTreePageId.ROOT_PTR) {
			parent = 0;
		}
		else {
			parent = id.getPageNumber();
		}
	}


	/**
	 * Returns the number of empty slots on this page.
	 */
	public abstract int getNumEmptySlots();
}

