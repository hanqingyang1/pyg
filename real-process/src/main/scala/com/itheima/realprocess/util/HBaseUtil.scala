package com.itheima.realprocess.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptor, _}
import org.apache.hadoop.hbase.util.Bytes

/**
  * HBase的工具类
  *
  * 获取Table
  * 保存单列数据
  * 查询单列数据
  * 保存多列数据
  * 查询多列数据
  * 删除数据
  */
object HBaseUtil {

  //创建Hbase配置,默认获取类路径下的hbase-site.xml配置文件
  val conf:Configuration = HBaseConfiguration.create()

  //创建连接
  val connection:Connection = ConnectionFactory.createConnection(conf)

  //获取Hbase操作类
  private val admin: Admin = connection.getAdmin


  /**
    * 获取表
    * @param tableNameStr 表名
    * @param columnFamilyName 列族名
     * @return
    */
  def getTable(tableNameStr:String,columnFamilyName:String):Table ={
    val tableName = TableName.valueOf(tableNameStr)
    //判断表名是否存在，不存在创建表
    if(!admin.tableExists(tableName)){
      //构建表描述
      val desc:TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)
      //构建列族描述
      val columnFamilyDescriptor:ColumnFamilyDescriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyName.getBytes).build()
      desc.setColumnFamily(columnFamilyDescriptor)
      //创建表
      admin.createTable(desc.build())
    }
    connection.getTable(tableName)
  }

  /**
    * 根据rowkey,列名查询数据
    * @param tableName 表名
    * @param rowkey rowkey
    * @param columnFamily 列蔟名
    * @param column 列名
    * @return 数据
    */
  def putData(tableNameStr:String,rowKey:String,columnFamilyName:String,columnName:String,columnValue:String): Unit ={
    //获取表
    val table = getTable(tableNameStr,columnFamilyName)
    //添加数据
    try {
      val put  = new Put(rowKey.getBytes())
      put.addColumn(columnFamilyName.getBytes(),columnName.getBytes(),columnValue.getBytes())
      //保存数据
      table.put(put)
    }catch{
      case ex:Exception =>{
        ex.printStackTrace()
      }
    }finally{
      table.close()
    }
  }

  /**
    * 根据rowkey,列名查询数据
    * @param tableName 表名
    * @param rowkey rowkey
    * @param columnFamily 列蔟名
    * @param column 列名
    * @return 数据
    */
  def getData(tableNameStr:String,rowKey:String,columnFamilyName:String,columnName:String):String={
    //获取表
    val table = getTable(tableNameStr,columnFamilyName)

    try {
      val get = new Get(rowKey.getBytes)
      val result = table.get(get)
      if(result != null && result.containsColumn(columnFamilyName.getBytes(),columnName.getBytes)){
        val bytes: Array[Byte] = result.getValue(columnFamilyName.getBytes,columnName.getBytes)
        Bytes.toString(bytes)
      }else{
        ""
      }
    }catch{
      case e: Exception => {
        e.printStackTrace()
        ""
      }

    }finally {
      table.close()
    }
  }

  /**
    * 使用Map封装数据，插入/更新一批数据
    * @param tableName 表名
    * @param rowKey rowkey
    * @param columnFamily 列蔟
    * @param mapData key:列名，value：列值
    */
  def putMapData(tableNameStr:String,rowKey:String,columnFamilyName:String,map: Map[String,Any]): Unit ={
    //获取表
    val table = getTable(tableNameStr,columnFamilyName)
    try {
      val put = new Put(rowKey.getBytes)
      for((k,v) <- map){
        put.addColumn(columnFamilyName.getBytes(),Bytes.toBytes(k),Bytes.toBytes(v.toString))

      }
      table.put(put)
    }catch {
      case ex:Exception => ex.printStackTrace()
    }finally{
      table.close()
    }
  }

  /**
    * 获取多列数据的值
    *
    * @param tableNameStr     表名
    * @param rowkey           rowkey
    * @param columnFamilyName 列族名
    * @param columnNameList   多个列名
    * @return 多个列名和多个列值的Map集合
    */
  def getMapData(tableNameStr: String, rowkey: String, columnFamilyName: String, columnNameList: List[String]): Map[String, String] = {

    // 1. 获取Table
    val table = getTable(tableNameStr, columnFamilyName)

    try{
      // 2. 构建Get
      val get = new Get(rowkey.getBytes)

      // 3. 执行查询
      val result: Result = table.get(get)

      // 4. 遍历列名集合,取出列值,构建成Map返回
      columnNameList.map {
        col =>
          val bytes: Array[Byte] = result.getValue(columnFamilyName.getBytes(), col.getBytes)

          if (bytes != null && bytes.size > 0) {
            col -> Bytes.toString(bytes)
          }else{
            ""->""
          }
      }.filter(_._1!="").toMap

    }catch{
      case ex:Exception=>{
        ex.printStackTrace()
        Map[String,String]()
      }
    }finally {
      // 5. 关闭Table
      table.close()
    }
  }


  /**
    * 根据rowkey删除一条数据
    *
    * @param tableNameStr 表名
    * @param rowkey rowkey
    */
  def deleteData(tableNameStr:String, rowkey:String, columnFamilyName:String) = {
    val tableName: TableName = TableName.valueOf(tableNameStr)

   val table = getTable(tableNameStr,columnFamilyName)
    try {
      val delete = new Delete(Bytes.toBytes(rowkey))
      table.delete(delete)
    }
    catch {
      case e:Exception => e.printStackTrace()
    }
    finally {
      table.close()
    }
  }


  def main(args: Array[String]): Unit = {
//    println(getTable("test", "info"))
    // 测试：插入一条数据
//    putData("test" , "123" , "info" , "tt" , "this is a test")

    // 测试查询一条数据
//     val data1 = getData("test", "123" , "info" , "tt")
//     println(data1)

    // 测试：插入一批数据
//    var map = Map("t1" -> "123", "t2" -> "234")
//     putMapData("test", "123", "info", map)
    println(getMapData("test", "123", "info", List("t1", "t2")))
  }
}















