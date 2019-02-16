package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
	private static final long serialVersionUID = 1L;
	OpIterator child;
	OpIterator[] children;
	int afield;
	int gfield;
	Aggregator.Op aop;
	TupleDesc aggreateTupleDesc;
	Aggregator aggregator;
	OpIterator aggregatorIterator;
	
	
	/**
	 * Constructor.
	 * 
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
	 * you with your implementation of readNext().
	 * 
	 * 
	 * @param child  The OpIterator that is feeding us tuples.
	 * @param afield The column over which we are computing an aggregate.
	 * @param gfield The column over which we are grouping the result, or -1 if
	 *               there is no grouping
	 * @param aop    The aggregation operator to use
	 */
	public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
		this.child = child;
		this.afield = afield;
		this.gfield = gfield;
		this.aop = aop;
		this.children=new OpIterator[] {child};
	
		
	}

	/**
	 * @return If this aggregate is accompanied by a groupby, return the groupby
	 *         field index in the <b>INPUT</b> tuples. If not, return
	 *         {@link simpledb.Aggregator#NO_GROUPING}
	 */
	public int groupField() {
		return gfield;
	}

	/**
	 * @return If this aggregate is accompanied by a group by, return the name of
	 *         the groupby field in the <b>OUTPUT</b> tuples. If not, return null;
	 */
	public String groupFieldName() {
		return child.getTupleDesc().getFieldName(gfield);
	}

	/**
	 * @return the aggregate field
	 */
	public int aggregateField() {
		return afield;
	}

	/**
	 * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
	 */
	public String aggregateFieldName() {
		return child.getTupleDesc().getFieldName(afield);
	}

	/**
	 * @return return the aggregate operator
	 */
	public Aggregator.Op aggregateOp() {
		return aop;
	}

	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		 return aop.toString();
	}

	public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
		super.open();
		child.open();
		Type groupbyFieldType=child.getTupleDesc().getFieldType(gfield);
		Type aggregateFieldType=child.getTupleDesc().getFieldType(afield);
		if(aggregateFieldType==Type.INT_TYPE) {
			 aggregator=new IntegerAggregator(gfield,groupbyFieldType,afield,aop);
		}else {
			 aggregator=new StringAggregator(gfield,groupbyFieldType,afield,aop);
		}
		while(child.hasNext()) {
			Tuple tup=child.next();
			Type aggregateField=tup.getField(afield).getType();
			aggregator.mergeTupleIntoGroup(tup);
			
		}
		aggregatorIterator=aggregator.iterator();
		aggregatorIterator.open();
	}

	/**
	 * Returns the next tuple. If there is a group by field, then the first field is
	 * the field by which we are grouping, and the second field is the result of
	 * computing the aggregate. If there is no group by field, then the result tuple
	 * should contain one field representing the result of the aggregate. Should
	 * return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		if(aggregatorIterator.hasNext()) {
			Tuple tup= aggregatorIterator.next();
			/*We need to converted the tuple fetched from the aggregator, modified its filed names and return the new tuple*/
			Tuple convertedTuple=new Tuple(getTupleDesc());
			Field aggregateField;
			aggregateField=tup.getField(1);
			if(gfield==Aggregator.NO_GROUPING) {
				convertedTuple.setField(0, aggregateField);
			}
			else {
				Field groupByField=tup.getField(0);
				convertedTuple.setField(0, groupByField);
				convertedTuple.setField(1, aggregateField);
			}
			return convertedTuple;
			
		}
		return null;
		
		
	}

	public void rewind() throws DbException, TransactionAbortedException {
		 child.rewind();
		 aggregatorIterator.rewind();
	}

	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field, this
	 * will have one field - the aggregate column. If there is a group by field, the
	 * first field will be the group by field, and the second will be the aggregate
	 * value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are given
	 * in the constructor, and child_td is the TupleDesc of the child iterator.
	 */
	public TupleDesc getTupleDesc() {
		if(aggreateTupleDesc!=null) {
			return aggreateTupleDesc;
		}
		
		Type groupbyFieldType=child.getTupleDesc().getFieldType(gfield);
		String groupbyName =child.getTupleDesc().getFieldName(gfield);
		String aggregateFieldname=child.getTupleDesc().getFieldName(afield);
		String aggregateColumnName=aop.toString()+"("+aggregateFieldname+")";
		Type[] aggregateTypes;
		String[] aggregateFieldNames;
		if(gfield==Aggregator.NO_GROUPING) {
			aggregateTypes=new Type[]{Type.INT_TYPE};
			aggregateFieldNames=new String[]{aggregateColumnName};
		}
		else {
			aggregateTypes=new Type[]{groupbyFieldType,Type.INT_TYPE};
			aggregateFieldNames=new String[] {groupbyName,aggregateColumnName};
		}
		aggreateTupleDesc= new TupleDesc(aggregateTypes,aggregateFieldNames);
		return aggreateTupleDesc;
	}

	public void close() {
		child.close();
		aggregatorIterator.close();
		
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
