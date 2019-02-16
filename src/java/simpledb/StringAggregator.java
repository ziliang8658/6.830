package simpledb;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.Map.Entry;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field,Integer> groupByMap;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what!=Op.COUNT) {
        	throw new IllegalArgumentException();
        }
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        groupByMap=new TreeMap<Field,Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	switch(what) {
    	case COUNT:
    		aggregateCount(tup);
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new aggregateIterator();
    }
    
    
    private class aggregateIterator implements OpIterator{
    	Iterator<Entry<Field,Integer>> aggregateTupleIterator;
    	TupleDesc aggregateTupleDesc;
    	public aggregateIterator() {
    		aggregateTupleIterator=null;
    		Type[] aggregateTypes=new Type[]{gbfieldtype,Type.INT_TYPE};
			String[] aggregateFieldNames=new String[] {"groupVal","aggregateVal"};
			aggregateTupleDesc=new TupleDesc(aggregateTypes,aggregateFieldNames);
    		
    	}
		@Override
		public void open() throws DbException, TransactionAbortedException {
			
			aggregateTupleIterator=groupByMap.entrySet().iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			
			if(aggregateTupleIterator==null) {
				return false;
			}
			return aggregateTupleIterator.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			Entry<Field,Integer> entry=aggregateTupleIterator.next();
			Tuple nextTuple=new Tuple(aggregateTupleDesc);
			nextTuple.setField(0, entry.getKey());
			Field valueField=new IntField(entry.getValue());
			nextTuple.setField(1,valueField);
			return nextTuple;
			
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			aggregateTupleIterator=groupByMap.entrySet().iterator();
			
		}

		@Override
		public TupleDesc getTupleDesc() {
			return aggregateTupleDesc;
		}

		@Override
		public void close() {
			aggregateTupleIterator=null;
		}
    	
    }
    
    private void aggregateCount(Tuple tuple) {
    	Field aField=tuple.getField(afield);
    	Field groupField;
    	if(gbfield==NO_GROUPING) {
    		groupField=new StringField("NO_GROUPING","NO_GROUPING".length());
    	}
    	else {
    		groupField=tuple.getField(gbfield);
    	}
    	if(aField instanceof StringField) {
    		Integer lastCountValue=groupByMap.get(groupField);
    		Integer currentCountValue;
    		if(lastCountValue==null) {
    			currentCountValue=new Integer(1);
    		}
    		else {
    			currentCountValue=lastCountValue+1;
    		}
    		groupByMap.put(groupField,currentCountValue);
    	
    	}
    	
    }
    
}
