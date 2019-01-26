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
	private File storedFile;
	private BufferedInputStream BufferedinputStream;
	private TupleDesc tupleDesc;
	private Byte[] HeapFileData;
	private final int tableId;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f the file that stores the on-disk backing store for this heap file.
	 */

	public HeapFile(File f, TupleDesc td) {
		this.storedFile = f;
		this.tableId = getId();
		try {
			this.BufferedinputStream= new BufferedInputStream(new FileInputStream(storedFile));
			this.tupleDesc = td;
		} catch (FileNotFoundException e) {
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
			BufferedinputStream.read(data,0,BufferPool.getPageSize());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (IndexOutOfBoundsException e) {
		}

		try {
			page = new HeapPage((HeapPageId) pid, data);
			if(pid.getPageNumber()==1) {
				System.out.println(data);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return page;

	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for lab1
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		return (int) Math.ceil((double) storedFile.length() / BufferPool.getPageSize());
	}
	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		return null;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		return null;
		// not necessary for lab1
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
		   if (currentPageIndex < numPages()-1) {
			   	currentPageIndex++;
				PageId currentPageId = new HeapPageId(tableId, currentPageIndex);
				currentPage = Database.getBufferPool().getPage(tid, currentPageId, Permissions.READ_WRITE);
				tupleIterator = ((HeapPage) currentPage).iterator();
				return tupleIterator.hasNext();
			}
		   else {
			   return false;
		   }

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
