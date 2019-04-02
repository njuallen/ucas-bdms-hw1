import java.util.ArrayList;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.log4j.*;

/**
 * Read two files from HDFS.
 * Join them and store the final results to HBase.
 * compile: javac Hw1Grp0
 * run: java Hw1Grp0 R=rFileName S=sFileName join:RColumn=SColumn res:SColumn,RColumn,RColumn
 * eg: java Hw1Grp0 R=/hw1/lineitem.tbl S=/hw1/orders.tbl join:R0=S0 res:S1,R1,R5
 * @author  Zhigang Liu
 * @version 1.0
 * @since   2019-04-02
 */
public class Hw1Grp0 {
  public static void main(String[] args) throws IOException, URISyntaxException{
    // DEBUG: output all input args
    for (int i = 0; i < args.length; i++)
      System.out.println(args[i]);

    // example command line args
    // java Hw1Grp0 R=/hw1/lineitem.tbl S=/hw1/orders.tbl join:R0=S0 res:S1,R1,R5
    if (args.length != 4) {
      System.out.println("Expect R file, S file, Join column and result column");
      return;
    }

    // parse arguments
    // first extract R file and S file name
    // skip "R=" and "S="
    String rFile = args[0].substring(2);
    String sFile = args[1].substring(2);
    System.out.printf("rFile: %s sFile: %s\n", rFile, sFile);

    // secondly, we extract join column index
    // first skip "join:", then split by "="
    String[] columns = args[2].substring(5).split("=");
    int rJoinColumnIndex = Integer.parseInt(columns[0].substring(1));
    int sJoinColumnIndex = Integer.parseInt(columns[1].substring(1));

    // DEBUG
    System.out.printf("rJoinColumnIndex: %d sJoinColumnIndex: %d\n", rJoinColumnIndex, sJoinColumnIndex);

    // we extract result column index
    // first skip "res:", then split by ","
    columns = args[3].substring(4).split(",");
    ResultColumn[] resultColumns = new ResultColumn[columns.length];

    // DEBUG
    System.out.printf("Result Columns:\n");

    for (int i = 0; i < columns.length; i++) {
      String fileName = columns[i].substring(0, 1);
      int columnIndex = Integer.parseInt(columns[i].substring(1));
      resultColumns[i] = new ResultColumn(fileName, columnIndex);
      // DEBUG
      System.out.printf("%s %d\n", fileName, columnIndex);
    }

    // DEBUG
    System.out.printf("Result Columns:\n");
    for (int i = 0; i < resultColumns.length; i++)
      System.out.println(resultColumns[i]);

    String hdfsRFile= "hdfs://localhost:9000/" + rFile;
    String hdfsSFile= "hdfs://localhost:9000/" + sFile;
    String localRFile= "/home/bdms/hw1/" + rFile;
    String localSFile= "/home/bdms/hw1/" + sFile;
    ArrayList rRecords = loadHDFSRecords(hdfsRFile);
    ArrayList sRecords = loadHDFSRecords(hdfsSFile);

    /*
       ArrayList<Record> rRecords = loadLocalRecords(localRFile);
       ArrayList<Record> sRecords = loadLocalRecords(localSFile);
       */

    // DEBUG
    System.out.println("rRecords:");
    for (int i = 0; i < rRecords.size(); i++)
      System.out.println(rRecords.get(i));
    System.out.println("sRecords:");
    for (int i = 0; i < sRecords.size(); i++)
      System.out.println(sRecords.get(i));

    ArrayList<JoinedRecord> joinedRecords = hashJoinRecords(7, rRecords,
        sRecords, rJoinColumnIndex, sJoinColumnIndex);

    // DEBUG
    System.out.println("joinedRecords:");
    for (int i = 0; i < joinedRecords.size(); i++)
      System.out.println(joinedRecords.get(i));

    ArrayList<FilteredRecord> filteredRecords = filterRecords(joinedRecords, resultColumns);

    // DEBUG
    System.out.println("filteredRecords:");
    for (int i = 0; i < filteredRecords.size(); i++)
      System.out.println(filteredRecords.get(i));

    putRecords(filteredRecords, resultColumns);
  }

   /**
    * Load records from HDFS
    * @param file HDFSFileName
    * @return records array of records
    * @exception IOException, URISyntaxException
    */
  private static ArrayList<Record> loadHDFSRecords(String file) throws IOException, URISyntaxException{

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(file), conf);
    Path path = new Path(file);
    FSDataInputStream in_stream = fs.open(path);

    BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
    ArrayList<Record> v = new ArrayList<Record>();
    String s;
    while ((s = in.readLine()) != null) {
      // columns are split by "|"
      String[] columns = s.split("\\|");
      v.add(new Record(columns));
    }

    in.close();
    fs.close();
    return v;
  }

   /**
    * Load records from local file system, used for debugging
    * @param file LocalFileName
    * @return records array of records
    * @exception IOException
    */
  private static ArrayList<Record> loadLocalRecords(String file) throws IOException {
    FileInputStream in_stream = new FileInputStream(file);
    BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
    ArrayList<Record> v = new ArrayList<Record>();
    String s;
    while ((s = in.readLine()) != null) {
      // columns are split by "|"
      String[] columns = s.split("\\|");
      v.add(new Record(columns));
    }

    in.close();
    return v;
  }

   /**
    * Join records with naive nested for loops
    * @param rRecords records from table 1
    * @param sRecords records from table 2
    * @param rJoinColumnIndex specify which column in table 1 is used to join
    * @param sJoinColumnIndex specify which column in table 2 is used to join
    * @return joinedRecords array of joinedRecords
    */
  private static ArrayList<JoinedRecord> loopJoinRecords(ArrayList<Record> rRecords,
      ArrayList<Record> sRecords, int rJoinColumnIndex, int sJoinColumnIndex) {
    ArrayList<JoinedRecord> v = new ArrayList<JoinedRecord>();
    // use a naive loop
    for (int i = 0; i < rRecords.size(); i++) {
      Record r = rRecords.get(i);
      for (int j = 0; j < sRecords.size(); j++) {
        Record s = sRecords.get(j);
        if (r.record[rJoinColumnIndex].equals(s.record[sJoinColumnIndex]))
          v.add(new JoinedRecord(r.record[rJoinColumnIndex], r, s));
      }
    }
    return v;
  }

   /**
    * Join records with hash join, first hash records into different buckets,
    * then do loop join for records in buckets.
    * @param bucketSize hash bucket size
    * @param rRecords records from table 1
    * @param sRecords records from table 2
    * @param rJoinColumnIndex specify which column in table 1 is used to join
    * @param sJoinColumnIndex specify which column in table 2 is used to join
    * @return joinedRecords array of joinedRecords
    */
  private static ArrayList<JoinedRecord> hashJoinRecords(int bucketSize, ArrayList<Record> rRecords,
      ArrayList<Record> sRecords, int rJoinColumnIndex, int sJoinColumnIndex) {
    // create hash buckets
    ArrayList<ArrayList<Record>> rBuckets = new ArrayList<ArrayList<Record>>();
    ArrayList<ArrayList<Record>> sBuckets = new ArrayList<ArrayList<Record>>();
    for (int i = 0; i < bucketSize; i++) {
      rBuckets.add(new ArrayList<Record>());
      sBuckets.add(new ArrayList<Record>());
    }

    // hash records into buckets
    for (int i = 0; i < rRecords.size(); i++) {
      Record r = rRecords.get(i);
      int hash = r.record[rJoinColumnIndex].hashCode() % bucketSize;
      if (hash < 0)
        hash += bucketSize;
      rBuckets.get(hash).add(r);
    }

    for (int i = 0; i < sRecords.size(); i++) {
      Record s = sRecords.get(i);
      int hash = s.record[sJoinColumnIndex].hashCode() % bucketSize;
      if (hash < 0)
        hash += bucketSize;
      sBuckets.get(hash).add(s);
    }

    // do join
    ArrayList<JoinedRecord> v = new ArrayList<JoinedRecord>();
    for (int i = 0; i < bucketSize; i++)
      v.addAll(loopJoinRecords(rBuckets.get(i), sBuckets.get(i), rJoinColumnIndex, sJoinColumnIndex));

    return v;
  }

   /**
    * Filter records, only return needed columns, discard others
    * @param joinedRecords joinedRecords
    * @param resultColumns the columns that you need
    * @return filterRecords filteredRecords
    */
  private static ArrayList filterRecords(ArrayList<JoinedRecord> joinedRecords,
      ResultColumn[] resultColumns) {
    ArrayList<FilteredRecord> v = new ArrayList<FilteredRecord>();
    // use a naive loop
    for (int i = 0; i < joinedRecords.size(); i++) {
      JoinedRecord record = joinedRecords.get(i);

      String key = record.key;
      Record r = record.rRecord;
      Record s = record.sRecord;
      String[] columns = new String[resultColumns.length];
      for (int j = 0; j < resultColumns.length; j++) {
        boolean isRFile = resultColumns[j].isRFile;
        int columnIndex = resultColumns[j].columnIndex;
        columns[j] = isRFile ? r.record[columnIndex] : s.record[columnIndex];
      }
      v.add(new FilteredRecord(key, new Record(columns)));
    }
    return v;
  }

   /**
    * Store final results to hbase
    * @param records final result
    * @param resultColumns the columns that you need
    * @return Nothing
    */
  private static void putRecords(ArrayList<FilteredRecord> records,
      ResultColumn[] resultColumns)
    throws MasterNotRunningException, ZooKeeperConnectionException, IOException {

    Logger.getRootLogger().setLevel(Level.WARN);


    // Instantiating configuration class
    Configuration conf = HBaseConfiguration.create();

    // Instantiating HBaseAdmin class
    HBaseAdmin admin = new HBaseAdmin(conf);

    String tableName = "Result";
    String columnFamilyName = "res";

    // if table already exists, we drop it
    if (admin.tableExists(tableName)) {
      System.out.println("Table exists");
      System.out.println("Delete table");
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }

    // create/recreate the table
    //creating table descriptor
    HTableDescriptor htd = new HTableDescriptor(tableName.getBytes());

    //creating column family descriptor
    HColumnDescriptor family = new HColumnDescriptor(columnFamilyName.getBytes());

    //adding coloumn family to HTable
    htd.addFamily(family);

    System.out.println("Create table");

    admin.createTable(htd);

    admin.close();

    // put filtered records to database
    HTable table = new HTable(conf, tableName);

    for (int i = 0; i < records.size(); i++) {
      FilteredRecord record = records.get(i);

      String rowKey = record.key;
      String []values = record.record.record;

      Put put = new Put(rowKey.getBytes());
      for (int j = 0; j < resultColumns.length; j++) {
        boolean isRFile = resultColumns[j].isRFile;
        int columnIndex = resultColumns[j].columnIndex;
        String key = (isRFile ? "R" : "S") + String.format("%d", columnIndex);
        String value = values[j];
        System.out.printf("put record: rowKey: %s key: %s value: %s\n",
            rowKey, key, value);
        put.add(columnFamilyName.getBytes(), key.getBytes(), value.getBytes());
      }
      table.put(put);
    }
    table.close();
  }
}

class ResultColumn {
  boolean isRFile;
  int columnIndex;

  ResultColumn(String fileName, int index) {
    isRFile = fileName.equals("R");
    columnIndex = index;
  }

  public String toString() {
    return String.format("(%s, %d)", isRFile ? "R" : "S", columnIndex);
  }
}

class Record {
  String[] record;

  Record(String[] r) {
    record = r;
  }

  public String toString() {
    String s = "<";
    for (int i = 0; i < record.length; i++) {
      s += record[i];
      if (i != record.length - 1)
        s += ", ";
    }
    s += ">";
    return s;
  }
}

class JoinedRecord {
  String key;
  Record rRecord;
  Record sRecord;

  JoinedRecord(String k, Record r, Record s) {
    key = k;
    rRecord = r;
    sRecord = s;
  }

  public String toString() {
    String s = "(";
    s += key + ", ";
    s += rRecord.toString() + ", ";
    s += sRecord.toString() + ")";
    return s;
  }
}

class FilteredRecord {
  String key;
  Record record;

  FilteredRecord(String k, Record r) {
    key = k;
    record = r;
  }

  public String toString() {
    String s = "(";
    s += key + ", ";
    s += record.toString() + ")";
    return s;
  }
}
