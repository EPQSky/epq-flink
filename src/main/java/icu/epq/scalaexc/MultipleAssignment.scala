/**
 * 元组和多重赋值
 *
 * @param primaryKey
 * @return
 */
def getPersonInfo(primaryKey: Int) = {
  ("Venkat", "Subramaniam", "venkats@agiledeveloper.com")
}

val (firstName, lastName, emailAddress) = getPersonInfo(1)

println(s"First Name: $firstName")
println(s"Last Name: $lastName")
println(s"Fall Name: ${firstName + "·" + lastName}")
println(s"Email Address: $emailAddress")

val info = getPersonInfo(1)

println(s"${info._1}")