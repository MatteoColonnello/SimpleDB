package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.server.SimpleDB;

public class ReplacementStrategyTest {
	
	public static void testNaive() {
	      SimpleDB db = new SimpleDB("examtest", 400, 4);
	      BufferMgr bm = db.bufferMgr();
	      Buffer[] buff = new Buffer[6];
	    		  
	      BufferMgr.setReplacementStrategy(ReplacementStrategy.NAIVE);
	      buff[0] = bm.pin(new BlockId("testfile", 1));
	      buff[1] = bm.pin(new BlockId("testfile", 2));
	      buff[2] = bm.pin(new BlockId("testfile", 3));
	      buff[3] = bm.pin(new BlockId("testfile", 4));
	      
	      bm.unpin(buff[3]);
	      bm.unpin(buff[1]);
	      
	      bm.pin(new BlockId("testfile", 5));
	      printBufferPool(bm.getBufferPool(), db.fileMgr());
	}
	
	public static void testLRU() {
	      SimpleDB db = new SimpleDB("examtest", 400, 4);
	      BufferMgr bm = db.bufferMgr();
	      Buffer[] buff = new Buffer[6];
	    		  
	      BufferMgr.setReplacementStrategy(ReplacementStrategy.LRU);
	      buff[0] = bm.pin(new BlockId("testfile", 1));
	      buff[1] = bm.pin(new BlockId("testfile", 2));
	      buff[2] = bm.pin(new BlockId("testfile", 3));
	      buff[3] = bm.pin(new BlockId("testfile", 4));
	      
	      bm.unpin(buff[3]);
	      bm.unpin(buff[1]);
	      bm.unpin(buff[0]);
	      bm.unpin(buff[2]);
	      
	      bm.pin(new BlockId("testfile", 5));
	      bm.pin(new BlockId("testfile", 6));
	      bm.pin(new BlockId("testfile", 7));
	      printBufferPool(bm.getBufferPool(), db.fileMgr());
	}

	public static void testClock() {
	      SimpleDB db = new SimpleDB("examtest", 400, 4);
	      BufferMgr bm = db.bufferMgr();
	      Buffer[] buff = new Buffer[6];
	    		  
	      BufferMgr.setReplacementStrategy(ReplacementStrategy.CLOCK);
	      buff[0] = bm.pin(new BlockId("testfile", 1));
	      buff[1] = bm.pin(new BlockId("testfile", 2));
	      buff[2] = bm.pin(new BlockId("testfile", 3));
	      buff[3] = bm.pin(new BlockId("testfile", 4));	      
	     	      
	      bm.unpin(buff[2]);
	      bm.pin(new BlockId("testfile", 5));

	      bm.unpin(buff[0]);
	      bm.unpin(buff[3]);
	      
	      bm.pin(new BlockId("testfile", 6));
	      bm.pin(new BlockId("testfile", 7));
	      printBufferPool(bm.getBufferPool(), db.fileMgr());
	}
	
	public static void testEsame20210303() {
		SimpleDB db = new SimpleDB("examtest", 400, 4);
	    BufferMgr bm = db.bufferMgr();
	    FileMgr fm = db.fileMgr();
	    Buffer[] buff = new Buffer[10];

	    /* costruzione dello stato iniziale */
		resetExamInitialState(bm, buff);
		System.out.println("TRACE NAIVE:");
		System.out.println("STATO INIZIALE:");
		printBufferPool(bm.getBufferPool(),fm);

		BufferMgr.setReplacementStrategy(ReplacementStrategy.NAIVE);
		applyChanges(bm, buff, fm);
		
		resetExamInitialState(bm, buff);
		System.out.println("TRACE LRU:");
		System.out.println("STATO INIZIALE:");
		printBufferPool(bm.getBufferPool(),fm);
		
		BufferMgr.setReplacementStrategy(ReplacementStrategy.LRU);
		applyChanges(bm, buff, fm);
		
		resetExamInitialState(bm, buff);
		System.out.println("TRACE LRU:");
		System.out.println("STATO INIZIALE:");
		printBufferPool(bm.getBufferPool(),fm);
		
		BufferMgr.setReplacementStrategy(ReplacementStrategy.CLOCK);
		applyChanges(bm, buff, fm);
		

	}
	
	private static void resetExamInitialState(BufferMgr bm, Buffer[] buff) {
		buff[0] = bm.pin(new BlockId("examfile", 70));
		buff[1] = bm.pin(new BlockId("examfile", 33));
		buff[2] = bm.pin(new BlockId("examfile", 35));
		buff[3] = bm.pin(new BlockId("examfile", 47));
		
		buff[0].setLoadTime(1);
		buff[1].setLoadTime(7);
		buff[2].setLoadTime(3);
		buff[3].setLoadTime(9);
		
		buff[0].setPins(1);
		buff[1].setPins(2);
		buff[2].setPins(0);
		buff[3].setPins(1);
		
		buff[2].setUnpinTime(8);
		
		buff[0].setModified(1, 0);
		
		BufferMgr.alterCounter(9);
	}

	private static void applyChanges(BufferMgr bm, Buffer[] buff, FileMgr fm) {
		System.out.println("10: unpin(70)");
		bm.unpin(buff[0]); // unpin 70
		printBufferPool(bm.getBufferPool(), fm);
		buff[4]=bm.pin(new BlockId("examfile", 60)); // pin 60
		System.out.println("11: pin(60)");
		printBufferPool(bm.getBufferPool(), fm);
		buff[4].setModified(1, 0); // setXXX(60,...)
		System.out.println("12: setXXX(60,...)");
		printBufferPool(bm.getBufferPool(), fm);
		bm.unpin(buff[4]); // unpin 60
		System.out.println("13: unpin(60)");
		printBufferPool(bm.getBufferPool(), fm);
		bm.flushAll(1); // flushAll
		System.out.println("14: flushAll");
		printBufferPool(bm.getBufferPool(), fm);
		buff[3].setModified(1, 0); // setXXX(47,...)
		System.out.println("15: setXXX(47,...)");
		printBufferPool(bm.getBufferPool(), fm);
		bm.unpin(buff[3]); // unpin 47
		System.out.println("16: unpin(47)");
		printBufferPool(bm.getBufferPool(), fm);
		buff[6]=bm.pin(new BlockId("examfile", 70)); // pin 70
		System.out.println("17: pin(70)");
		printBufferPool(bm.getBufferPool(), fm);
		buff[6].setModified(1, 0); // setXXX(70,...)
		System.out.println("18: setXXX(70,...)");
		printBufferPool(bm.getBufferPool(), fm);
		bm.unpin(buff[6]); // unpin 70
		System.out.println("19: unpin(70)");
		printBufferPool(bm.getBufferPool(), fm);
		buff[7]=bm.pin(new BlockId("examfile", 60)); // pin 60
		System.out.println("20: pin(60)");
		printBufferPool(bm.getBufferPool(), fm);
		buff[7].unpin(); // unpin 60
		System.out.println("21: unpin(60)");
		printBufferPool(bm.getBufferPool(), fm);
		buff[8]=bm.pin(new BlockId("examfile", 70)); // pin 70
		System.out.println("22: pin(70)");
		printBufferPool(bm.getBufferPool(), fm);
	}


	
	 public static void main(String[] args) throws Exception {
	     //testNaive();
	     testLRU();
	     //testClock();
		 //testEsame20210303();
	 
	   }
	 
	 private static void printBufferPool(Buffer[] bufferPool, FileMgr fm) {
		 for (int i=0; i<bufferPool.length; i++) {
	         Buffer b = bufferPool[i]; 
	         if (b != null) {
	        	 BlockId block = b.block();
	        	 long unpinTime = b.getUnpinTime();
	        	 long loadTime = b.getLoadTime();
	        	 int pins = b.getPins();
	        	 System.out.println("bufferPool["+i+"]: "+ 
	        			 "PINS: " + pins + ", " + 
	        			 "BLOCK " + block.number() +  ", " +
	        			 "LOAD TIME: " + loadTime + ", " + 
	        			 "UNPIN TIME: " + unpinTime + ", " +
	        			 "MODIFIED: " + b.isModified());
	
	         }  
	      }
		 System.out.println("File Manager LOG: \n" + fm.getBlockStats());
	 }
}
