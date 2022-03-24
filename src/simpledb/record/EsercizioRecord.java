package simpledb.record;

import java.util.HashSet;
import java.util.Set;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class EsercizioRecord {
	
	public static void main(String[] args) {
		
		  SimpleDB db = new SimpleDB("tabletest", 2000, 500); // 500 pagine nel buffer da 2000 byte
	      Transaction tx = db.newTx();

	      Schema sch = new Schema(); // uno schema R(A,B), dove A è un intero e B una stringa
	      sch.addIntField("A");
	      sch.addStringField("B", 12);
	      Layout layout = new Layout(sch);
	      for (String fldname : layout.schema().fields()) { // stampa degli offset
	         int offset = layout.offset(fldname);
	         System.out.println(fldname + " has offset " + offset);
	      }
	      System.out.println("RL=" + layout.slotSize());
	             
	      System.out.println("Inserimento 100.000 record.");
	      db.fileMgr().getBlockStats().reset();
	      TableScan ts = new TableScan(tx, "T", layout);
	      for (int i=0; i<100000;  i++) {
	         ts.insert();
	         ts.setInt("A", i);
	         ts.setString("B", "rec"+i);
	      }

	      System.out.println(db.fileMgr().getBlockStats());
	      db.fileMgr().getBlockStats().reset();
	      
	      
	      System.out.println("Cancellazione dei record per i quali A<20.000.\n");
	      int count = 0;
	      ts.beforeFirst();
	      while (ts.next()) {
	         int a = ts.getInt("A");
	         if (a < 20000) {
	            count++;
	            ts.delete();
	         }
	      }
	      System.out.println(count + " valori inferiori a 20000 cancellati.\n");
	      System.out.println(db.fileMgr().getBlockStats());
	      db.fileMgr().getBlockStats().reset();
	      
	      System.out.println("Inserimento 10.000 record con numeri casuali tra 0 e 100 per A.");
	      ts.beforeFirst();
	      int n=0;
	      for (int i=0; i<10000;  i++) {
	    	 n = (int) ((Math.random() * 100));
	         ts.insert();
	         ts.setInt("A", n);
	         ts.setString("B", "rec"+i);
	      }
	      System.out.println(db.fileMgr().getBlockStats());
	      db.fileMgr().getBlockStats().reset();
	      
	      /* generazione 500 numeri casuali tra 0 e 1000 */
	      Set<Integer> randomNumbers = new HashSet<>();
	      while(randomNumbers.size()<500)
	    	  randomNumbers.add((int) (Math.random() * 1000));

	      System.out.println("Conteggio delle ennuple con valori per A nella lista.\n");
	      
	      count = 0;
	      ts.beforeFirst();
	      while (ts.next()) {
	         int a = ts.getInt("A");
	         if(randomNumbers.contains(a))
	            count++;
	         }
	      
	      System.out.println(count + " valori della lista individuati.\n");
	      System.out.println(db.fileMgr().getBlockStats());
	      db.fileMgr().getBlockStats().reset();
		}

}
