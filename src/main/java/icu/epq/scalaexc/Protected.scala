class Vehicle {
  protected def checkEngine() {}
}

class Car extends Vehicle {
  def start() {
    checkEngine() /* 编译正确 */
  }

  def tow(car: Car) {
    car.checkEngine() // 编译正确
  }

  def tow(vehicle: Vehicle) {
    vehicle.checkEngine() // 编译错误
  }
}

class GasStation {
  def fillGas(vehicle: Vehicle) {
    vehicle.checkEngine() // 编译错误
  }
}