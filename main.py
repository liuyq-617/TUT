#!/usr/bin/python
###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################
# install pip
# pip install src/connector/python/

# -*- coding: utf-8 -*-
from util.resource import *



if __name__ == '__main__':
    tdRes.init()
    tdRes.getResourceList()   #获取资源列表
    tdRes.getResourceState()  #获取资源状态
    tdRes.getTestcase()       #获取所有的测试用例
    tdRes.reset()             #重置所有的资源
    tdRes.excuteCase("basic.py")        #执行测试用例
    tdRes.reset()


