package simpledb;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.lang.Integer;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final short ValueIndex=0;
    private static final short CountIndex=1;
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    Map<Field,int[]> groupByMap;
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        groupByMap=new TreeMap<Field,int[]>();
      

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	
        switch(what) {
        case MIN:
        	aggregateMin(tup);
        	break;
        case MAX:
        	aggregateMax(tup);
        	break;
        case AVG:
        	aggregateAvg(tup);
        	break;
        case SUM:
        	aggregateSum(tup);
        	break;
        case COUNT:
        	aggregateCount(tup);
        	break;
        }
    }
    

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
    	return new aggregateIterator();
    }
    
    

    
    private class aggregateIterator implements OpIterator{
    	Iterator<Entry<Field,int[]>> aggregateTupleIterator;
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
			Entry<Field,int[]> entry=aggregateTupleIterator.next();
			Tuple nextTuple=new Tuple(aggregateTupleDesc);
			nextTuple.setField(0, entry.getKey());
			Field valueField=new IntField(entry.getValue()[ValueIndex]);
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
    private void aggregateMin(Tuple tuple) {
    	Field aField=tuple.getField(afield);
    	int currentValue=((IntField)aField).getValue();
    	Field groupField;
    	if(gbfield==NO_GROUPING) {
    		groupField=new StringField("NO_GROUPING","NO_GROUPING".length());
    	}
    	else {
    		groupField=tuple.getField(gbfield);
    	}
    	if(aField instanceof IntField) {
    		int[] lastminValueArray=groupByMap.get(groupField);
    		
    		if(lastminValueArray==null||lastminValueArray[0]>currentValue) {
    			int[] currentValueArray=new int[] {currentValue,1};
    			groupByMap.put(groupField,currentValueArray);
    		}
    	
    	}
   }

 
    private void aggregateMax(Tuple tuple) {
    	
    	Field aField=tuple.getField(afield);
    	int currentValue=((IntField)aField).getValue();
     	Field groupField;
    	if(gbfield==NO_GROUPING) {
    		groupField=new StringField("NO_GROUPING","NO_GROUPING".length());
    	}
    	else {
    		groupField=tuple.getField(gbfield);
    	}
    	if(aField instanceof IntField) {
    		int[] lastmaxValueArray=groupByMap.get(groupField);
    		
    		if(lastmaxValueArray==null||lastmaxValueArray[0]<currentValue) {
    			int[] currentValueArray=new int[] {currentValue,1};
    			groupByMap.put(groupField,currentValueArray);
    		}
    	
    	}
    	
    }
    private void aggregateAvg(Tuple tuple) {
    	Field aField=tuple.getField(afield);
    	int currentValue=((IntField)aField).getValue();
    	Field groupField;
    	if(gbfield==NO_GROUPING) {
    		groupField=new StringField("NO_GROUPING","NO_GROUPING".length());
    	}
    	else {
    		groupField=tuple.getField(gbfield);
    	}
    	if(aField instanceof IntField) {
    		int[] lastAvgValueArray=groupByMap.get(groupField);
    		int[] currentAvgValueArray;
    		if(lastAvgValueArray==null) {
    			currentAvgValueArray=new int[] {currentValue,1};
    		}
    		else {
    			int currentAvgValue=(lastAvgValueArray[0]*lastAvgValueArray[1]+currentValue)/(lastAvgValueArray[1]+1);
    			currentAvgValueArray=new int[] {currentAvgValue,lastAvgValueArray[1]+1};
    		}
    		groupByMap.put(groupField,currentAvgValueArray );
    		
    	
    	}
    	
    }
    
    
    private void aggregateSum(Tuple tuple) {

    	Field aField=tuple.getField(afield);
    	int currentValue=((IntField)aField).getValue();
    	Field groupField;
    	if(gbfield==NO_GROUPING) {
    		groupField=new StringField("NO_GROUPING","NO_GROUPING".length());
    	}
    	else {
    		groupField=tuple.getField(gbfield);
    	}
    	if(aField instanceof IntField) {
    		int[] lastSumValueArray=groupByMap.get(groupField);
    		int[] currentSumValueArray;
    		if(lastSumValueArray==null) {
    			currentSumValueArray=new int[] {currentValue,1};
    		}
    		else {
    			int currentSumValue=lastSumValueArray[0]+currentValue;
    			currentSumValueArray=new int[] {currentSumValue,lastSumValueArray[1]+1};
    		}
    		groupByMap.put(groupField,currentSumValueArray);
    		
    	
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
    	if(aField instanceof IntField) {
    		int[] lastCountValueArray=groupByMap.get(groupField);
    		int[] currentCountValueArray;
    		if(lastCountValueArray==null) {
    			currentCountValueArray=new int[] {1,0};
    		}
    		else {
    			int currentCountValue=lastCountValueArray[0]+1;
    			currentCountValueArray=new int[] {currentCountValue,0};
    		}
    		groupByMap.put(groupField,currentCountValueArray);
    	
    	}
    	
    }
    

}
