# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from util.resource import *


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    tdRes.init()
    tdRes.getResourceList()   #获取资源列表
    tdRes.getResourceState()  #获取资源状态
    tdRes.getTestcase()       #获取所有的测试用例
    tdRes.reset()             #重置所有的资源
    tdRes.excuteCase("basic.py")        #执行测试用例
    tdRes.reset()



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
