/**
 * 传递变长参数 类似于..arg
 */

def max(values: Int*) = values.foldLeft(values(0)) {
  Math.max
}

val number = Array(2, 5, 3, 7, 1, 6)
max(number: _*)