import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetRed extends VoltProcedure {
  public final SQLStmt QueryRed = new SQLStmt("select * from input where ? - TIME <= ? and ? - TIME >= 0;");

  public VoltTable[] run(long T, long Tn) throws VoltAbortException {
	voltQueueSQL(QueryRed,Tn,T,Tn);
	return voltExecuteSQL();
  }
}
