/*Windowed stream Word count example which demonstrates ValueStateDescriptor*/
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingStateDescriptor,ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}

object QueryableStateWordCount {
  case class WordWithCount(word: String, count: Long)
  def main(args: Array[String]) : Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream("localhost", 9000)
    val windowCounts = text
      .flatMap(_.split("\\s"))
      .map(w => WordWithCount(w,1))
      .keyBy("word")
      .timeWindow(Time.seconds(20))
      .sum("count")
   
    val wordCountStateDesc = new ValueStateDescriptor[WordWithCount](
      "word-count-state",
      TypeInformation.of(new TypeHint[WordWithCount]() {}),
      new WordWithCount("the",1))
      
    val queryableStream = windowCounts
      .keyBy("word")
      .asQueryableState("word-count-stream", wordCountStateDesc)

    env.execute("Socket Window WordCount")
  }
}
  