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

/**
 *complie Hw1Grp0.java
 *
 * javac Hw1Grp0.java
 *
 *execute Hw1Grp0.java
 *
 * java Hw1Grp0
 *
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
    /*
       ArrayList rRecords = loadRecords(hdfsRFile);
       ArrayList sRecords = loadRecords(hdfsSFile);
       */
    ArrayList<Record> rRecords = loadLocalRecords(localRFile);
    ArrayList<Record> sRecords = loadLocalRecords(localSFile);

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
  }

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
      rBuckets.get(hash).add(r);
    }

    for (int i = 0; i < sRecords.size(); i++) {
      Record s = sRecords.get(i);
      int hash = s.record[sJoinColumnIndex].hashCode() % bucketSize;
      sBuckets.get(hash).add(s);
    }

    // do join
    ArrayList<JoinedRecord> v = new ArrayList<JoinedRecord>();
    for (int i = 0; i < bucketSize; i++)
      v.addAll(loopJoinRecords(rBuckets.get(i), sBuckets.get(i), rJoinColumnIndex, sJoinColumnIndex));

    return v;
  }

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

  // store to database
  private static void storeRecords(ArrayList records) {
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
