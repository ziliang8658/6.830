package simpledb;

public class LinkedPage implements Page{
	private LinkedPage first;
	private LinkedPage last;
	private LinkedPage head;
	@Override
	public PageId getId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TransactionId isDirty() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void markDirty(boolean dirty, TransactionId tid) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public byte[] getPageData() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Page getBeforeImage() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setBeforeImage() {
		// TODO Auto-generated method stub
		
	}

	
}
