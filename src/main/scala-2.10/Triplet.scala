/**
  * Created by sinash on 5/16/16.
  */
class Triplet(v: String, i: Long, p: Float) {

  var value: String = v
  var index: Long = i
  var probability: Float = p


  override def toString(): String = "(" + value + ", " + index + ", " + probability + ")"

}
