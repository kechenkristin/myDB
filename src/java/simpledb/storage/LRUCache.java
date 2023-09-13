package simpledb.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class LRUCache {

    private static class LinkNode {
        PageId pageId;
        Page page;
        LinkNode prev;
        LinkNode next;

        public LinkNode(PageId pageId, Page page) {
            this.pageId = pageId;
            this.page = page;
        }
    }
    private Map<PageId, LinkNode> map;
    private int capacity;
    private LinkNode head;
    private LinkNode tail;


    public LRUCache(int capacity) {
        this.capacity = capacity;
        map = new ConcurrentHashMap<>(capacity);
        head = new LinkNode(new HeapPageId(-1, -1), null);
        tail = new LinkNode(new HeapPageId(-1, -1), null);
        head.next = tail;
        tail.prev = head;
    }

    private void remove(LinkNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        map.remove(node.pageId);
    }



    private void addToHead(LinkNode node) {
        LinkNode oldFirst = head.next;
        head.next = node;
        node.prev = head;
        node.next = oldFirst;
        oldFirst.prev = node;
        map.put(node.pageId, node);
    }

    public void deleteLastNode() {
        remove(tail.prev);
    }

    public void moveNodeToHead(LinkNode node){
        remove(node);
        addToHead(node);
    }

    /**
     *
     * @return The size of current LRUCache
     */
    public int getSize() {
        return map.size();
    }

    /**
     * returns true if the LRUCache is oversize
     */
    public boolean overSize() {
        return getSize() >= capacity;
    }

    /**
     * returns true if the cache contains the page with the given pageId
     */
    public boolean containsPage(PageId pageId) {
        return map.containsKey(pageId);
    }
}