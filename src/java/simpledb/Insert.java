package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final int tableId;
    private  OpIterator[] children;
    private final TransactionId tid;
    private final TupleDesc resultCountTupleDesc;
    private boolean alreadyInserted;
    

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes her
    	this.tid=t;
    	this.children= new OpIterator[] {child};
    	this.tableId=tableId;
    	Type[] resultCountTupleType=new Type[] {Type.INT_TYPE};
    	String [] resultCountTupleName=new String[] {"Affected tuples Count"};
    	resultCountTupleDesc=new TupleDesc(resultCountTupleType,resultCountTupleName);
    	alreadyInserted=false;
    }

    public TupleDesc getTupleDesc() {
    	return resultCountTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
    	children[0].open();
    }

    public void close() {
    	super.close();
    	children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	children[0].rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	Tuple resultTuple=new Tuple(resultCountTupleDesc);
    	if(alreadyInserted) {
    		return null;
    	}
    	int count=0;
    	try {
    		while(children[0].hasNext()) {
    			Database.getBufferPool().insertTuple(tid,tableId,children[0].next());	
    			count++;
    		}
    		Field resultCountFiled=new IntField(count);
    		resultTuple.setField(0, resultCountFiled);
    		
    	}
    	catch(IOException e) {
    		e.printStackTrace();
    	}
    	alreadyInserted=true;
    	return resultTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.children=children;
    }
}
