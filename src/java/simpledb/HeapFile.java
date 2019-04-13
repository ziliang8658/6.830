package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private RandomAccessFile randomAccessFile;
	private File storedFile;
	private TupleDesc tupleDesc;
	private Byte[] HeapFileData;
	private final int tableId;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f the file that stores the on-disk backing store for this heap file.
	 */

	public HeapFile(File f, TupleDesc td) {
		storedFile = f;
		this.tableId = getId();
		try {
			randomAccessFile = new RandomAccessFile(f, "rw");
			tupleDesc = td;
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return storedFile;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note: you
	 * will need to generate this tableid somewhere to ensure that each HeapFile has
	 * a "unique id," and that you always return the same value for a particular
	 * HeapFile. We suggest hashing the absolute file name of the file underlying
	 * the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		return this.storedFile.getAbsolutePath().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return this.tupleDesc;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		int pageNumber = pid.getPageNumber();
		byte[] data = new byte[BufferPool.getPageSize()];
		Page page = null;
		try {
			randomAccessFile.seek(pageNumber * BufferPool.getPageSize());
			randomAccessFile.read(data);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			page = new HeapPage((HeapPageId) pid, data);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return page;

	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		byte[] pageData = page.getPageData();
		int pageSize = BufferPool.getPageSize();
		int offSet = page.getId().getPageNumber() * pageSize;
		randomAccessFile.write(pageData, offSet, pageSize);
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		return (int) storedFile.length() / BufferPool.getPageSize();
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		ArrayList<Page> affectedPages = new ArrayList<Page>();
		for (int pageNo = 0; pageNo != numPages(); pageNo++) {
			PageId pageId = new HeapPageId(tableId, pageNo);
			Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
			HeapPage heapPage = (HeapPage) page;
			if (heapPage.getNumEmptySlots() >= 1) {
				heapPage.insertTuple(t);
				affectedPages.add(page);
				return affectedPages;
			}
		}
		byte[] newHeapPageData = HeapPage.createEmptyPageData();
		HeapPageId newHeapPageId = new HeapPageId(tableId, numPages());
		HeapPage newPage = new HeapPage(newHeapPageId, newHeapPageData);
		newPage.insertTuple(t);
		randomAccessFile.seek((numPages()) * BufferPool.getPageSize());
		randomAccessFile.write(newPage.getPageData());
		affectedPages.add(newPage);
		return affectedPages;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		RecordId recordId = t.getRecordId();
		PageId pageId = recordId.getPageId();
		Page pageofTuple = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
		t.getRecordId();
		int tupleNumber = recordId.getTupleNumber();
		if (pageofTuple instanceof HeapPage) {
			((HeapPage) pageofTuple).deleteTuple(t);
		}
		ArrayList<Page> affectedPages = new ArrayList<Page>();
		affectedPages.add(pageofTuple);
		return affectedPages;

	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// some code goes here
		return new HeapFileIterator(tid);
	}

	private class HeapFileIterator implements DbFileIterator {
		public int currentPageIndex;
		Iterator<Tuple> tupleIterator;
		Page currentPage;
		TransactionId tid;

		public HeapFileIterator(TransactionId tid) {
			currentPageIndex = 0;
			this.tid = tid;
			currentPage = null;
			tupleIterator = null;
		}

		public void open() throws DbException, TransactionAbortedException {
			PageId currentPageId = new HeapPageId(tableId, 0);
			currentPage = Database.getBufferPool().getPage(tid, currentPageId, Permissions.READ_ONLY);
			tupleIterator = ((HeapPage) currentPage).iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			if (tupleIterator == null) {
				return false;
			}
			if (tupleIterator.hasNext()) {
				return true;
			}
			if (currentPageIndex < numPages() - 1) {
				currentPageIndex++;
				PageId currentPageId = new HeapPageId(tableId, currentPageIndex);
				currentPage = Database.getBufferPool().getPage(tid, currentPageId, Permissions.READ_WRITE);
				tupleIterator = ((HeapPage) currentPage).iterator();
				return tupleIterator.hasNext();
			}
			return false;

		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (tupleIterator == null) {
				throw new NoSuchElementException();
			}
			Object nextElement = tupleIterator.next();
			return (Tuple) nextElement;

		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			PageId currentPageId = new HeapPageId(tableId, 0);
			currentPage = Database.getBufferPool().getPage(tid, currentPageId, Permissions.READ_ONLY);
			tupleIterator = ((HeapPage) currentPage).iterator();
		}

		@Override
		public void close() {
			tupleIterator = null;
			currentPage = null;
		}

	}

}
