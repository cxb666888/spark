package spark.util

/**
  * @Auther: cxb
  * @Date: 2018/9/7 16:15
  * @Description:
  */
object SexUtil {

  def tranSex(sex: String): String = {
    var sexNews = ""
    if (sex != "" && sex != null) {
      if (sex.equals("Female") || sex.contains("0")) {
        sexNews = "0" //表示女性
      } else {
        sexNews = "1"
      }
    }
    sexNews
  }

}
