package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private  OpIterator[] children;
    private final TransactionId tid;
    private final TupleDesc resultCountTupleDesc;
    private boolean alreadyDeleted;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
    	this.tid=t;
    	this.children= new OpIterator[] {child};
    	Type[] resultCountTupleType=new Type[] {Type.INT_TYPE};
    	String [] resultCountTupleName=new String[] {"Affected tuples Count"};
    	resultCountTupleDesc=new TupleDesc(resultCountTupleType,resultCountTupleName);
    	alreadyDeleted=false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	Tuple resultTuple=new Tuple(resultCountTupleDesc);
    	if (alreadyDeleted) {
    		return null;
    	}
    	int count=0;
    	try {
    		while(children[0].hasNext()) {
    			Database.getBufferPool().deleteTuple(tid, children[0].next());
    			count++;
    		}
    		Field resultCountFiled=new IntField(count);
    		resultTuple.setField(0, resultCountFiled);
    		
    	}
    	catch(IOException e) {
    		e.printStackTrace();
    	}
    	alreadyDeleted=true;
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
