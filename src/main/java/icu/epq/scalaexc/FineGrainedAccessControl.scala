package society {
  package professional {

    class Executive {
      private[professional] var workDetails = null
      private[society] var friends = null
      private[this] var secrets = null

      def help(another: Executive): Unit = {
        println(another.workDetails)
        println(secrets)
        println(another.secrets) // 编译错误
      }
    }

    class Assistant {
      def assist(anExec: Executive): Unit = {
        println(anExec.workDetails)
        println(anExec.friends)
      }
    }

  }

  package social {

    class Acquaintance {
      def socialize(person: professional.Executive) {
        println(person.friends)
        println(person.workDetails) // 编译错误
      }
    }

  }


}