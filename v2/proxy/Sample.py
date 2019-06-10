from proxy.ProxyFactory import *


@ProxyFactory(InvocationHandler)
class Sample:
    def __init__(self, age):
        self.age = age

    def foo(self):
        print('hello', self.age)

    def add(self, x, y):
        return x + y


# s = Sample(12)
# print(s)
# print(type(s))
# s.foo()
# s.add(1, 2)
# s.add(2, 4)
# print(s.age)
