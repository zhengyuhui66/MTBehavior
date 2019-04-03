package com.hik.btbehavior

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import com.hiklife.utils.{ByteUtil, HBaseUtil}
import net.sf.json.{JSONArray, JSONObject}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

object App {
  val MTBehavior="MTBehavior"
  val MacRecoder="MacRecoder"
  val MacRecoder_dateInx="MacRecoder_dateInx"
  def main(args: Array[String]): Unit = {
    val path=args(0)
    val conf = new SparkConf().setAppName("MTBehavior")
    //conf.setMaster("local")
    val sc=new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val hBaseUtil = new HBaseUtil(path+"hbase/hbase-site.xml")
    hBaseUtil.createTable(MTBehavior,"BRT")
    val broadList = sc.broadcast(List(path+"hbase/hbase-site.xml",MacRecoder,MTBehavior))
    val MacData = query(path + "hbase/hbase-site.xml", MacRecoder_dateInx, spark.sparkContext)
    MacData.flatMap(x=>{
      x._2.listCells()
    }).map(x=>{
      Bytes.toString(CellUtil.cloneValue(x)).replace("\n","").replace("\r","")
    }).mapPartitions(partiton=>{
      val conn = ConnectionFactory.createConnection(HBaseUtil.getConfiguration(broadList.value(0).asInstanceOf[String]))
      val macRecorderTable = conn.getTable(TableName.valueOf(broadList.value(1).asInstanceOf[String])).asInstanceOf[HTable]
      macRecorderTable.setAutoFlush(false, false)
      macRecorderTable.setWriteBufferSize(5 * 1024 * 1024)
      val formatStr=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      partiton.map(line=>{
        val g:Get=new Get(line.getBytes)
        val r:Result=macRecorderTable.get(g)
        val b=r.getValue("RD".getBytes,"IN".getBytes)
        val j:JSONObject= JSONObject.fromObject(new String(b))
        val rt=j.get("rt").toString
        val mac=j.get("mac").toString.replace("-","")
        val ct=j.get("ct").toString
        val devID=j.get("devID")
        val columnName=getColumnName(formatStr.parse(ct))
        (mac+CommFunUtils.SPLIT+columnName+CommFunUtils.SPLIT+devID,(rt.toInt,1))
      })
    })
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(x=>{
        val rowkeys=x._1.split(CommFunUtils.SPLIT)
        val mac=rowkeys(0)
        val columnTime=rowkeys(1)
        val devId=rowkeys(2)
        val rt=x._2._1
        val suc=x._2._2
        val tJson:JSONObject = new JSONObject()
        tJson.accumulate("devID",devId)
        tJson.accumulate("suc",suc)
        tJson.accumulate("rt",rt)
      (mac+CommFunUtils.SPLIT+columnTime,tJson.toString)
      })
      .groupByKey().map(x=>{
        val itertor= x._2.iterator
        val tJsonArray=new JSONArray()
        while(itertor.hasNext) {
          tJsonArray.add(itertor.next())
        }
        (x._1,tJsonArray.toString)
    }).foreachPartition(partitionOfRecords => {
      val conn = ConnectionFactory.createConnection(HBaseUtil.getConfiguration(broadList.value(0).asInstanceOf[String]))
      val table = conn.getTable(TableName.valueOf(broadList.value(2).asInstanceOf[String])).asInstanceOf[HTable]
      table.setAutoFlush(false, false)
      table.setWriteBufferSize(5 * 1024 * 1024)
      partitionOfRecords.foreach(record => {
        val value=record._2
        val keys=record._1.split(CommFunUtils.SPLIT)
        val mac=keys(0)
        val columnName=keys(1)
        val mTime:Calendar=Calendar.getInstance()
        val put = new Put(mac.getBytes)
        put.addColumn("BRT".getBytes, columnName.getBytes, value.getBytes)
        table.put(put)
      })
      table.flushCommits()
      table.close()
      conn.close()
    })
    }
  /**
    * Éú³ÉHBASE RDD
    * @param epc
    * @param hbasepath
    * @param tableName
    * @param sc
    * @return
    */
  def query(hbasepath: String, tableName: String, sc: SparkContext): RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result)] ={
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("RD"))
    scan.addColumn(Bytes.toBytes("RD"),Bytes.toBytes("IN"))

    val mTime:Calendar=Calendar.getInstance()
    mTime.add(Calendar.DATE,-1)
    mTime.set(Calendar.HOUR,23)
    mTime.set(Calendar.MINUTE,59)
    mTime.set(Calendar.SECOND,59)

    val formatStr:SimpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val rowkey=getPrex(formatStr.format(mTime.getTime))
    val startrowkey=rowkey+getPrexTime(formatStr.format(mTime.getTime))
    scan.setStartRow(startrowkey.getBytes)
    mTime.add(Calendar.DATE,-1)
    val stoprowkey=rowkey+getPrexTime(formatStr.format(mTime.getTime))
    scan.setStopRow(stoprowkey.getBytes)
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray)
    val hconf = HBaseConfiguration.create()
    hconf.addResource(new Path(hbasepath))

    hconf.set(TableInputFormat.INPUT_TABLE, tableName)
    hconf.set(TableInputFormat.SCAN, ScanToString)
    val hBaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD
  }

  def getPrex(datetime:String):String={
    val dateTimes=datetime.substring(0,10).replace("-","")
    CommFunUtils.byte2HexStr(CommFunUtils.GetHashCodeWithLimit(dateTimes, 0xFF).toByte)
  }

  def getPrexTime(datetime:String):String={
    val bb = new Array[Byte](4)
    ByteUtil.putInt(bb, ("2524608000".toLong - CommFunUtils.Str2Date(datetime).getTime / 1000).asInstanceOf[Int], 0)
    CommFunUtils.byte2HexStr(bb)
  }


  def getColumnName(d:Date):String={
    val i:SimpleDateFormat = new SimpleDateFormat("yyMMddHH")
    val c:Calendar=Calendar.getInstance();
    c.setTime(d)
    val minute=if(c.get(Calendar.MINUTE)>=30){
                "30"
              }else{
                "00"
              }
    i.format(d)+minute
  }
}
