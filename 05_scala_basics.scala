SCALA
=======
-- spark-shell opens a prompt for spark and scala
-- one of the features of scala is type inference. It derives the data type automatically.
-- 2 types of variables
		Immutable variables: defined by val
		Mutable variable: defined by var
-- Scala is statically typed language and Python is dynamically typed language
-- List is an immutable collection type. Values can never be changed	
-- Array is a mutable collection type. The value at a particular index can be changed. However the values can't be added. Array can't be expanded.
-- ArrayBuffer is mutable collection type and we can add elements at any particular index.
-- Map is a key-value pair type of collection. 
		It can't be referenced by index. 
		Reference can only be done via the key. 
		It's an immutable map. 
		There can't be duplicate keys in map. The key value pairs can't be changed. However new key-value pairs can be added/removed.
-- Map - Mutable collection can change the names of the key value pairs.
-- Tuple - it is an indexed collection. It is a heterogenous collection of different data types
		Index starts with 1 instead of 0 like other collections.
-- Set - Set always have unique elements. Even if duplicates are inserted, it stores unique values.



		
		
scala> 10/4
res3: Int = 2
scala> 12/7.0
res4: Double = 1.7142857142857142
scala> 'a'
res5: Char = a
scala> "Spark"
res6: String = Spark
scala> "a"
res7: String = a
scala> 'aa'
<console>:1: error: unclosed character literal
       'aa'
          ^

scala> "1. Immutable Variables: val\n 2. Mutable Variables: var"
res8: String = 
1. Immutable Variables: val
 2. Mutable Variables: var
 
scala> val x = "Spark"
x: String = Spark
scala> x = "S"
<console>:27: error: reassignment to val
         x = "S"
           ^
scala> var x = "Spark"
x: String = Spark
scala> x = "S"
x: String = S


scala> "Statically Typed Language"
res9: String = Statically Typed Language

scala> var x = "Spark"
x: String = Spark
scala> x = "Scala"
x: String = Scala
scala> x
res10: String = Scala
scala> x = 10
<console>:27: error: type mismatch;
 found   : Int(10)
 required: String
         x = 10
             ^

scala> var num = List(1,2,3,4,5,6)
num: List[Int] = List(1, 2, 3, 4, 5, 6)

scala> num(0)
res0: Int = 1

scala> num(3)
res1: Int = 4

scala> num(4)
res2: Int = 5

scala> num(10)
java.lang.IndexOutOfBoundsException: 10
  at scala.collection.LinearSeqOptimized.apply(LinearSeqOptimized.scala:63)
  at scala.collection.LinearSeqOptimized.apply$(LinearSeqOptimized.scala:61)
  at scala.collection.immutable.List.apply(List.scala:86)
  ... 29 elided

scala> num(4+)
<console>:13: error: type mismatch;
 found   : (x: Double)Double <and> (x: Float)Float <and> (x: Long)Long <and> (x: Int)Int <and> (x: Char)Int <and> (x: Short)Int <and> (x: Byte)Int <and> (x: String)String
 required: Int
       num(4+)
            ^

scala> num.head
res5: Int = 1

scala> num.tail
res6: List[Int] = List(2, 3, 4, 5, 6)

scala> num.sum
res7: Int = 21

scala> num.take(2)
res8: List[Int] = List(1, 2)

scala> num.take(4)
res9: List[Int] = List(1, 2, 3, 4)

scala> num.distinct
res10: List[Int] = List(1, 2, 3, 4, 5, 6)

scala> num.add(7)
<console>:13: error: value add is not a member of List[Int]
       num.add(7)
           ^

scala> num.size
res12: Int = 6

scala> num.reverse
res13: List[Int] = List(6, 5, 4, 3, 2, 1)

scala> num.min
res14: Int = 1

scala> num.max
res15: Int = 6

scala> num.last
res16: Int = 6

scala> num.first
<console>:13: error: value first is not a member of List[Int]
       num.first
           ^

scala> num.isEmpty
res18: Boolean = false


scala> var str = List("ABC","DEF")
str: List[String] = List(ABC, DEF)

scala> str
res19: List[String] = List(ABC, DEF)

scala> str(0)
res20: String = ABC

scala> str(1)
res21: String = DEF

scala> str.min
res22: String = ABC

scala> var arr = Array(1,2,3,4)
arr: Array[Int] = Array(1, 2, 3, 4)

scala> arr(0)
res23: Int = 1

scala> arr.size
res24: Int = 4

scala> arr.min
res25: Int = 1

scala> arr.max
res26: Int = 4

scala> arr.head
res27: Int = 1

scala> arr.tail
res28: Array[Int] = Array(2, 3, 4)

scala> arr(0) = 10

scala> arr
res30: Array[Int] = Array(10, 2, 3, 4)

scala> 

scala> import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuffer

scala> var cars = ArrayBuffer[String]()
cars: scala.collection.mutable.ArrayBuffer[String] = ArrayBuffer()

scala> cars.size
res31: Int = 0

scala> cars += "BMW"
res32: scala.collection.mutable.ArrayBuffer[String] = ArrayBuffer(BMW)

scala> cars
res33: scala.collection.mutable.ArrayBuffer[String] = ArrayBuffer(BMW)

scala> cars.size
res34: Int = 1

scala> cars += "Merc"
res35: scala.collection.mutable.ArrayBuffer[String] = ArrayBuffer(BMW, Merc)

scala> cars += "Lamborgini"
res36: scala.collection.mutable.ArrayBuffer[String] = ArrayBuffer(BMW, Merc, Lamborgini)

scala> cars
res37: scala.collection.mutable.ArrayBuffer[String] = ArrayBuffer(BMW, Merc, Lamborgini)

scala> cars.size
res38: Int = 3

scala> cars.insert(1,"Bentley")

scala> cars
res40: scala.collection.mutable.ArrayBuffer[String] = ArrayBuffer(BMW, Bentley, Merc, Lamborgini)

scala> cars.insert(1, "Fiat", "Volvo", "Jaguar")

scala> cars
res42: scala.collection.mutable.ArrayBuffer[String] = ArrayBuffer(BMW, Fiat, Volvo, Jaguar, Bentley, Merc, Lamborgini)

scala> cars(1)
res43: String = Fiat

scala> cars.size
res44: Int = 7

scala> cars.trim
<console>:14: error: value trim is not a member of scala.collection.mutable.ArrayBuffer[String]
       cars.trim
            ^

scala> cars.trimEnd(2)

scala> cars
res47: scala.collection.mutable.ArrayBuffer[String] = ArrayBuffer(BMW, Fiat, Volvo, Jaguar, Bentley)

scala> cars.remove(2)
res48: String = Volvo

scala> cars
res49: scala.collection.mutable.ArrayBuffer[String] = ArrayBuffer(BMW, Fiat, Jaguar, Bentley)

scala> cars(0) = "VOLVO"

scala> cars
res51: scala.collection.mutable.ArrayBuffer[String] = ArrayBuffer(VOLVO, Fiat, Jaguar, Bentley)

scala> 



scala> var country = Map("IN" -> "India")
country: scala.collection.immutable.Map[String,String] = Map(IN -> India)

scala> country
res52: scala.collection.immutable.Map[String,String] = Map(IN -> India)

scala> country.size
res53: Int = 1

scala> country += Map("CH" -> "China")
<console>:14: error: value += is not a member of scala.collection.immutable.Map[String,String]
  Expression does not convert to assignment because:
    type mismatch;
     found   : scala.collection.immutable.Map[String,String]
     required: (String, String)
    expansion: country = country.+(Map("CH".$minus$greater("China")))
       country += Map("CH" -> "China")
               ^

scala> country += ("CH" -> "China")

scala> country
res56: scala.collection.immutable.Map[String,String] = Map(IN -> India, CH -> China)

scala> country.size
res57: Int = 2

scala> country.keys
res58: Iterable[String] = Set(IN, CH)

scala> country.values
res59: Iterable[String] = MapLike.DefaultValuesIterable(India, China)

scala> country += ("SG" -> "Singapore")

scala> country.keys
res61: Iterable[String] = Set(IN, CH, SG)

scala> country("IN")
res62: String = India

scala> country("India")
java.util.NoSuchElementException: key not found: India
  at scala.collection.immutable.Map$Map3.apply(Map.scala:156)
  ... 29 elided

scala> country
res2: scala.collection.immutable.Map[String,String] = Map(IN -> India, CH -> China, SG -> Singapore)

scala> country -= "CH"

scala> country
res4: scala.collection.immutable.Map[String,String] = Map(IN -> India, SG -> Singapore)

scala> country("IN")
res5: String = India

scala> country("IN") = "Bharat"
<console>:13: error: value update is not a member of scala.collection.immutable.Map[String,String]
       country("IN") = "Bharat"
       ^

scala> var state = scala.collection.mutable.Map("MP" -> "Madhya Pradesh")
state: scala.collection.mutable.Map[String,String] = Map(MP -> Madhya Pradesh)

scala> state
res8: scala.collection.mutable.Map[String,String] = Map(MP -> Madhya Pradesh)

scala> state("MP")
res9: String = Madhya Pradesh

scala> state += ("RJ" -> "Rajasthan")
res10: scala.collection.mutable.Map[String,String] = Map(RJ -> Rajasthan, MP -> Madhya Pradesh)

scala> state
res11: scala.collection.mutable.Map[String,String] = Map(RJ -> Rajasthan, MP -> Madhya Pradesh)

scala> state(1)
<console>:13: error: type mismatch;
 found   : Int(1)
 required: String
       state(1)
             ^

scala> state.head
res13: (String, String) = (RJ,Rajasthan)

scala> state.tail
res14: scala.collection.mutable.Map[String,String] = Map(MP -> Madhya Pradesh)

scala> state("MP") = "M.P"

scala> state
res16: scala.collection.mutable.Map[String,String] = Map(RJ -> Rajasthan, MP -> M.P)

scala> state("MP")
res17: String = M.P

scala> var t = (1, "Spark", 500.50)
t: (Int, String, Double) = (1,Spark,500.5)

scala> t
res18: (Int, String, Double) = (1,Spark,500.5)

scala> t._1
res19: Int = 1

scala> t._3
res21: Double = 500.5

scala> t._2
res22: String = Spark

scala> t._1 = 10
<console>:12: error: reassignment to val
       t._1 = 10
            ^

scala> var s = Set(1,2,3,4,3,2,1)
s: scala.collection.immutable.Set[Int] = Set(1, 2, 3, 4)

scala> s
res24: scala.collection.immutable.Set[Int] = Set(1, 2, 3, 4)

scala> s.head
res25: Int = 1

scala> s.tail
res26: scala.collection.immutable.Set[Int] = Set(2, 3, 4)

scala> s.min
sres27: Int = 1

scala> s.max
res29: Int = 4

scala> s.sum
res30: Int = 10

scala> s.last
res31: Int = 4

scala> s.size
res32: Int = 4
			
scala> var t = Set(3,4,5,6)
t: scala.collection.immutable.Set[Int] = Set(3, 4, 5, 6)

scala> t
res33: scala.collection.immutable.Set[Int] = Set(3, 4, 5, 6)

scala> s.union(t)
res34: scala.collection.immutable.Set[Int] = Set(5, 1, 6, 2, 3, 4)

scala> s
res35: scala.collection.immutable.Set[Int] = Set(1, 2, 3, 4)

scala> t
res36: scala.collection.immutable.Set[Int] = Set(3, 4, 5, 6)

scala> s.intersect(t)
res37: scala.collection.immutable.Set[Int] = Set(3, 4)

scala> s.minus(t)
<console>:14: error: value minus is not a member of scala.collection.immutable.Set[Int]
       s.minus(t)

scala> s.diff(t)
res39: scala.collection.immutable.Set[Int] = Set(1, 2)

scala> t.diff(s)
res40: scala.collection.immutable.Set[Int] = Set(5, 6)	   
			
			
			
-- difference between val and var in collections
scala> val x = List(1,2,3,4)
x: List[Int] = List(1, 2, 3, 4)
scala> x
res0: List[Int] = List(1, 2, 3, 4)
scala> var y = List(1,2,3,4)
y: List[Int] = List(1, 2, 3, 4)
scala> y
res1: List[Int] = List(1, 2, 3, 4)
scala> x(0)
res2: Int = 1
scala> y(0)
res3: Int = 1
scala> x(0) = 10
<console>:28: error: value update is not a member of List[Int]
              x(0) = 10
              ^
scala> y(10) = 10
<console>:28: error: value update is not a member of List[Int]
              y(10) = 10
              ^
scala> x = List(2,3,4,5)
<console>:27: error: reassignment to val
         x = List(2,3,4,5)
           ^
scala> y = List(2,3,4,5)
y: List[Int] = List(2, 3, 4, 5)
scala> x
res6: List[Int] = List(1, 2, 3, 4)
scala> y
res7: List[Int] = List(2, 3, 4, 5)			


The list is immutable here. However, the reference to the list x and y, which are val and var respectively. x is immutable and y is mutable.
