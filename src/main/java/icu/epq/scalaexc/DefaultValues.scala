/**
 * 为参数提供默认值
 */

def mail(destination: String = "head office", mailClass: String = "first"): Unit = println(s"sending to $destination by $mailClass class")

mail("Houston office", "Priority")
mail("Boston office")
mail()

mail(mailClass = "Priority", destination = "Bahamas office")

mail(mailClass = "Priority")