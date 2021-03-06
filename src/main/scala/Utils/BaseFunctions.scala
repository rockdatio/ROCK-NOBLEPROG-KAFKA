package Utils

import java.lang.reflect.Modifier

import com.google.gson.{GsonBuilder, JsonObject}

object BaseFunctions extends Serializable {
  private val gson = new GsonBuilder()
    .setDateFormat("yyyy-MM-dd HH:mm:ss-0000")
    .excludeFieldsWithModifiers(Modifier.FINAL, Modifier.TRANSIENT, Modifier.STATIC)
    .create()

  def getJson(record: String): JsonObject = {
    val x: JsonObject = gson.fromJson(record, classOf[JsonObject])
    x
  }

  def getAmountRandom(start: Int, end: Int): Int = {
    val rnd = new scala.util.Random
    val result: Int = start + rnd.nextInt((end - start) + 1)
    result
  }
}
