package cn.just.project.spark.dao

import cn.just.project.spark.domain.CourseClickCount
import cn.just.spark.utils.HBaseUtils
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 访问数据库DAO层
  * 今天到现在为止的实战课程的访问量
  */
object CourseClickCountDao {

  val tableName = "course_clickcount"    //表名
  val cf = "info"                         //列簇
  val qualifer = "clickcount"               //列名


  /**
   * 向数据库中保存数据
   * @param tableName 表名
   * @param rowKey  rowkey
   * @param cf  列簇
   * @param column  列名
   * @param colValue 列值
   */
  def save2(tableName : String, rowKey: String, cf: String, column: String, colValue :Long): Long ={
    // hbase(main):003:0> put 'test', 'row1', 'cf:a', 'value1'
    // hbase(main):004:0> put 'test', 'row2', 'cf:b', 'value2'
    // 其中'test' 是表名, 'row1'、'row2' 都是row唯一键, 'cf:a'、'cf:b' 是 列簇, 'value1'、'value2' 是列值
    val table = HBaseUtils.getInstance().getTable(tableName)
    table.incrementColumnValue(
      Bytes.toBytes(rowKey),      //rowkey
      Bytes.toBytes(cf),                  //列簇
      Bytes.toBytes(column),            //列名
      colValue                     //值
    )
  }
  /**
    * 向数据库中保存数据
    * @param courseList
    */
  def save(courseList : ListBuffer[CourseClickCount]): Unit ={
      val table = HBaseUtils.getInstance().getTable(tableName)
      for(ele <- courseList){
        /**
         * Atomically increments a column value.
        以原子方式递增列值。

                If the column value already exists and is not a big-endian long, this could throw an exception.
        如果列值已存在且不是大端长度，则可能引发异常。

                If the column value does not yet exist it is initialized to amount and written to the specified column.
        如果列值尚不存在，则将其初始化为amount并写入指定列。

                Setting durability to Durability.
        将耐久性设置为耐久性。

                SKIP_WAL means that in a fail scenario you will lose any increments that have not been flushed.
        SKIP_WAL 意味着在失败场景中，您将丢失所有未刷新的增量。
                Params:
                row – The row that contains the cell to increment.
                family – The column family of the cell to increment.
                qualifier – The column qualifier of the cell to increment.
                amount – The amount to increment the cell with (or decrement, if the amount is negative).
                durability – The persistence guarantee for this increment.
                Returns:
                The new value, post increment.
         */
        table.incrementColumnValue(
          Bytes.toBytes(ele.day_course),      //rowkey
          Bytes.toBytes(cf),                  //列簇
          Bytes.toBytes(qualifer),            //列名
          ele.clickcount                      //值
        )
      }
  }

  /**
    * 从数据中读取信息
    * @param day_course
    * @return
    */
  def get(day_course:String):Long={
    val table=HBaseUtils.getInstance().getTable(tableName)

    val get=new Get(Bytes.toBytes(day_course))      //rowkey

    val value=table.get(get).getValue(Bytes.toBytes(cf),Bytes.toBytes(qualifer))

    if(value==null){
      0l
    }else{
      Bytes.toLong(value)
    }
  }


  def main(args: Array[String]): Unit = {
//    val list=new ListBuffer[CourseClickCount]
//    list.append(CourseClickCount("20180724_45",85))
//    list.append(CourseClickCount("20180724_124",45))
//    list.append(CourseClickCount("20180724_189",12))
//
//    save(list)

    println(get("20180724_125"))


  }

}
