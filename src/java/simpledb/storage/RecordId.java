package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PageId pageId;
    private final int tupleNo;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageId of the page on which the tuple resides
     * @param tupleNo
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleNo) {
        pageId = pid;
        this.tupleNo = tupleNo;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return tupleNo;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (! (o instanceof RecordId toCompare)) {
            return false;
        }
        // RecordId toCompare = (RecordId) o;

        return this.pageId.equals(toCompare.pageId) && this.tupleNo == toCompare.getTupleNumber();
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        int prime = 31;
        return pageId.hashCode() * prime + tupleNo;
    }
}
