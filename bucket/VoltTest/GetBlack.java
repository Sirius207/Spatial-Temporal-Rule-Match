import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetBlack extends VoltProcedure {
  public final SQLStmt QueryRed = new SQLStmt("select * from input where ? - TIME <= ?*2 and ? - TIME > ?;");

  public VoltTable[] run(long T, long Tn) throws VoltAbortException {
	voltQueueSQL(QueryRed,Tn,T,Tn,T);
	return voltExecuteSQL();
  }
}
