package simpledb.file;

import java.util.HashMap;
import java.util.Map;

public class BlockStats {
	
	private Map<String,Integer> writtenBlocksPerFile = new HashMap<>();
	private Map<String,Integer> readBlocksPerFile = new HashMap<>();
	
	public void logReadBlock(BlockId block) {
		if(this.readBlocksPerFile.putIfAbsent(block.fileName(), 1)!=null) {
			this.readBlocksPerFile.put(block.fileName(), this.readBlocksPerFile.get(block.fileName())+1);
		}
	}
		
	public void logWrittenBlock(BlockId block) {
		if(this.writtenBlocksPerFile.putIfAbsent(block.fileName(), 1)!=null) {
			this.writtenBlocksPerFile.put(block.fileName(), this.writtenBlocksPerFile.get(block.fileName())+1);
		}	
	}
	
	public void reset() {
		this.writtenBlocksPerFile.clear();
		this.readBlocksPerFile.clear();
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("READ: \n");
		buf.append(this.readBlocksPerFile.toString());
		buf.append("\n");
		buf.append("WRITTEN: \n");
		buf.append(this.writtenBlocksPerFile.toString());
		buf.append("\n");
		return buf.toString();
	}
}
