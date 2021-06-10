/**
 * 多行原始字符串
 */

val str =
  """In his famous inaugural speech, John F. Kennedy said
"And so, my fellow Americans: ask not what your country can do
for you-ask what you can do for your country." He then proceeded
to speak to the citizens of the World..."""

println(str)

val stra =
  """In his famous inaugural speech, John F. Kennedy said
    A|"And so, my fellow Americans: ask not what your country can do
    A|for you-ask what you can do for your country." He then proceeded
    A|to speak to the citizens of the World...""".stripMargin('A')
println(stra)

val discount = 10.011
val product = "ticket"
val price = 25.12

println(f"On $product $discount%2.1f%% saves $$${price * discount/100.00}%2.2f")