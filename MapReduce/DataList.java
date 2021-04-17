package PollutionSummary;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class DataList implements Writable {
	private int ozone;
	private int pm;
	private int cm;
	private int sd;
	private int nd;
	public DataList(int ozone, int pm, int cm, int sd, int nd) {
		this.ozone = ozone;
		this.pm = pm;
		this.cm = cm;
		this.sd = sd;
		this.nd = nd;
	}
	public int getOzone() {return ozone;}
	public int getPM() {return pm;}
	public int getCM() {return cm;}
	public int getSD() {return sd;}
	public int getND() {return nd;}
	@Override
	public void readFields(DataInput in) throws IOException {
		ozone = in.readInt();
		pm = in.readInt();
		cm = in.readInt();
		sd = in.readInt();
		nd = in.readInt();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(ozone);
		out.writeInt(pm);
		out.writeInt(cm);
		out.writeInt(sd);
		out.writeInt(nd);
		
	}
	
	public String toString() {
		return ozone + ", " + pm + ", " + cm + ", " + sd + ", " + nd;
	}
	
	
}
