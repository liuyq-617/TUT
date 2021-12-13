###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import os
import time
import datetime
from typing_extensions import final
from util.log import *
import yaml
import paramiko


class TDResource:
    def __init__(self):
        self.server = {}
        self.client = {}
        self.serverlist = []
        self.clientlist = []
        self.testcases = {}
        self.testgroup = set()

    def init(self):
        cfg = self.readYaml("res", "dev.yaml")
        self.server = cfg[0]["server"]
        self.client = cfg[1]["client"]

    def readYaml(self, root, file):
        yamlPath = os.path.join(root, file)
        try:
            with open(yamlPath, "r") as f:
                temp = f.read()
        except:
            print(f"read resource file {yamlPath} failed")
            exit(-1)
        yamlresult = yaml.load(stream=temp, Loader=yaml.FullLoader)
        # print(yamlresult)
        return yamlresult

    def addCase(self):
        pass

    def getStatus(self, distTemp, name):
        return distTemp[name]

    def getResourceList(self):
        self.serverlist = list(self.server.keys())
        self.clientlist = list(self.client.keys())

    def getResourceState(self) -> dict:
        serverStatus = {}
        clientStatus = {}
        for i in self.serverlist:
            serverStatus[i] = self.server[i]["idle"]
        for i in self.clientlist:
            clientStatus[i] = self.client[i]["idle"]
        return serverStatus, clientStatus

    def getTestcase(self):
        for root, dirs, files in os.walk("tests"):
            for file in files:
                if file.endswith("yaml"):
                    self.handleCaseFile(root, file)
        # print(self.testcases, self.testgroup)

    def handleCaseFile(self, root, file):
        ctemp = self.readYaml(root, file)
        self.testcases[ctemp[0]["name"]] = ctemp[1]
        self.testgroup.add(ctemp[1]["group"])

    def getExcuteNode(self, servernum, clientnum):
        serverIdleList = self.whichIdle(self.server)
        clientIdleList = self.whichIdle(self.client)
        if len(serverIdleList) < servernum or len(clientIdleList) < clientnum:
            print(
                "Server or Client resource are insufficient, please wait for a moument "
            )
            exit(-1)
        clientlist = []
        print("client num:%d", clientnum)
        if clientnum == 0:
            clientlist.append(serverIdleList[0])
        else:
            clientlist.append(clientIdleList[:clientnum])
            self.updateResource(clientIdleList, "client", "idle", 1)
        serverlist = serverIdleList[:servernum]
        self.updateResource(serverlist, "server", "idle", 1)
        return serverIdleList[:servernum], clientlist

    def updateResource(self, reslist, restype, properties, value):
        """更新资源文件

        Args:
            reslist ([list]): 待更新资源列表
            restype ([char]): 更新资源的类型，目前有server，client
            properties ([char]): 更新资源的属性
            value ([type]): 更新资源的值
        """
        for i in reslist:
            if restype == "server":
                self.server[i][properties] = value
            if restype == "client":
                self.client[i][properties] = value
        server = {}
        client = {}
        server["server"] = self.server
        client["client"] = self.client
        self.writeToYaml([server, client], os.path.join("res", "dev.yaml"))

    def reset(self):
        """重置所有的资源为空闲状态"""
        self.updateResource(self.serverlist, "server", "idle", 0)
        self.updateResource(self.clientlist, "client", "idle", 0)

    def writeToYaml(self, content, file):
        try:
            with open(file, "w") as f:
                yaml.dump(content, f)
        except:
            print(f"wirteToYaml: {file} failed")
        print(f"wirteToYaml: {file} succeed")

    def excuteCase(self, casename):
        try:
            caseEnv = self.testcases[casename]["env"]
        except:
            print(f"{casename} doesn't exits!!")
            exit(-1)
        serverlist, clientlist = self.getExcuteNode(
            caseEnv["server"], caseEnv["client"]
        )
        print(f"excute node :\n\tserver:{serverlist}\n\tclient:{clientlist}")
        print("Start deploy server and client ,version:{}".format(caseEnv["version"]))
        print(caseEnv)
        if caseEnv["clean"]:
            self.cleanRemoteEnv(serverlist, clientlist)  # 清理环境
        self.deploy(serverlist, clientlist, caseEnv["version"])
        pass

    def deploy(self, serverlist, clientlist, version):
        for i in serverlist:
            self.remoteCmd(i, list("ls"), "server")
        for i in clientlist:
            if i not in self.clientlist:
                continue
            self.remoteCmd(i, list("ls"), "client")

    def cleanRemoteEnv(self, serverlist, clientlist):
        cmdList = [
            "rmtaos || echo 'taso not install'",
            "rm -rf /var/lib/taos",
            "rm -rf /var/log/taos",
        ]
        for i in serverlist:
            self.remoteCmd(i, cmdList, "server")
        for i in clientlist:
            if i not in self.clientlist:
                continue
            self.remoteCmd(i, cmdList, "client")

    def remoteCmd(self, node, cmd, type):
        temp = {}
        if type == "server":
            temp = self.server
        elif type == "client":
            temp = self.client
        host = temp[node]["FQDN"]
        username = temp[node]["username"]
        passwd = temp[node]["password"]
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(hostname=host, port=22, username=username, password=passwd)
        except:
            print(f"connect to {host} failed")
        else:
            print(f"connect to {host} succeed")
            for i in cmd:
                print("=" * 30, host, ": ", i, "start", "=" * 30)
                stdin, stdout, stderr = client.exec_command(i)
                result = stdout.read().decode("utf-8")
                err = stderr.read().decode("utf-8")
                if err:
                    print("error:{}".format(err))
                else:
                    print(result)
                print("=" * 30, host, ": ", i, "finish", "=" * 30)
        finally:
            print("=" * 30, host, " has finished", "finish", "=" * 30)
            client.close
        pass

    def whichIdle(self, dev) -> list:
        idleList = []
        for i in dev:
            if dev[i]["idle"] == 0:
                idleList.append(i)
        return idleList

    def close(self, keepProgress):
        self.sub.close(keepProgress)

    def consume(self):
        self.result = self.sub.consume()
        self.result.fetch_all()
        self.consumedRows = self.result.row_count
        self.consumedCols = self.result.field_count
        return self.consumedRows

    def checkRows(self, expectRows):
        if self.consumedRows != expectRows:
            tdLog.exit(
                "consumed rows:%d != expect:%d" % (self.consumedRows, expectRows)
            )
        tdLog.info("consumed rows:%d == expect:%d" % (self.consumedRows, expectRows))


tdRes = TDResource()
