# -*- coding: utf-8 -*-
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


import sys
import os
import time
import datetime
from typing_extensions import final
from util.log import *
import yaml
from fabric2 import Connection
import requests


class TDResource:
    def __init__(self):
        self.server = {}
        self.client = {}
        self.serverlist = []
        self.clientlist = []
        self.testcases = {}
        self.testgroup = set()
        self.env = []

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
            print("read resource file {} failed".format(yamlPath))
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

    def getResourceState(self):
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

    def updateEnv(self, casename, serverlist, clientlist):
        env = {}
        env["excuteCase"] = casename
        env["server"] = serverlist
        env["client"] = clientlist
        self.env.append(env)

    def excuteCase(self, casename):
        try:
            caseEnv = self.testcases[casename]["env"]
        except:
            print(f"{casename} doesn't exits!!")
            exit(-1)
        serverlist, clientlist = self.getExcuteNode(
            caseEnv["server"], caseEnv["client"]
        )
        self.updateEnv(casename, serverlist, clientlist)
        print(f"excute node :\n\tserver:{serverlist}\n\tclient:{clientlist}")
        print("Start deploy server and client ,version:{}".format(caseEnv["version"]))
        print(caseEnv)
        if caseEnv["clean"]:
            cleanpath = [caseEnv["dataDir"], caseEnv["logDir"]]
            self.cleanRemoteEnv(serverlist, clientlist, cleanpath)  # 清理环境
        self.deploy(serverlist, clientlist, caseEnv["version"])
        pass

    def deploy(self, serverlist, clientlist, version):
        for i in serverlist:
            self.installTaos(i, version, "server")
        for i in clientlist:
            if i not in self.clientlist:
                continue
            self.installTaos(i, version, "client")

    def installTaos(self, node, version, type):
        taosdPath, taosPath = self.downloadTaosd(version)
        cmd = [
            "cd /tmp",
            "tar zxf TDengine-%s-%s.tar.gz" % (type, version),
            "cd TDengine-%s-%s" % (type, version),
            'echo -en "\n\n"|./install*.sh',
        ]
        if type == "server":
            self.remotePut(node, taosdPath, "/tmp", type)
        elif type == "client":
            self.remotePut(node, taosPath, "/tmp", type)
        self.remoteCmd(node, cmd, type)

    def downloadTaosd(self, version):
        url_prifix = "https://www.taosdata.com/assets-download/TDengine-"
        url_suffix = "-Linux-x64.tar.gz"
        if (int(version.split(".")[1]) % 2) == 0:
            server = requests.get("".join([url_prifix, "server-", version, url_suffix]))
            client = requests.get("".join([url_prifix, "client-", version, url_suffix]))
        else:
            server = requests.get(
                "".join([url_prifix, "server-", version, "-beta", url_suffix])
            )
            client = requests.get(
                "".join([url_prifix, "client-", version, "-beta", url_suffix])
            )
        if server.status_code != 200 or client.status_code != 200:
            print("can't get taosd,quit!!")
            exit(-1)
        with open("TDengine-server-{}.tar.gz".format(version), "wb") as f:
            f.write(server.content)
        with open(f"TDengine-client-{version}.tar.gz", "wb") as f:
            f.write(client.content)
        return os.path.join(
            os.getcwd(), f"TDengine-server-{version}.tar.gz"
        ), os.path.join(os.getcwd(), f"TDengine-client-{version}.tar.gz")

    def cleanRemoteEnv(self, serverlist, clientlist, filelist):
        cmdList = [
            "rmtaos || echo 'taos not install'",
        ]
        for i in filelist:
            cmdList.append(f"rm -rf {i}")
        for i in serverlist:
            self.remoteCmd(i, cmdList, "server")
        for i in clientlist:
            if i not in self.clientlist:
                continue
            self.remoteCmd(i, cmdList, "client")

    def remotePut(self, node, file, path, type):
        temp = {}
        if type == "server":
            temp = self.server
        elif type == "client":
            temp = self.client
        host = temp[node]["FQDN"]
        username = temp[node]["username"]
        passwd = temp[node]["password"]
        try:
            with Connection(
                host, user=username, connect_kwargs={"password": passwd}
            ) as c:
                print(file,path,host)
                result = c.put(file, path)
        except:
            print("err on %s put%s",host,file)
        finally:
            print("=" * 30, host, " has finished", "=" * 30)

    def remoteCmd(self, node, cmd, type):
        temp = {}
        if type == "server":
            temp = self.server
        elif type == "client":
            temp = self.client
        host = temp[node]["FQDN"]
        username = temp[node]["username"]
        passwd = temp[node]["password"]
        try:
            with Connection(
                host, user=username, connect_kwargs={"password": passwd}
            ) as c:
                # if not cmd[0].startswith("cd"):
                #     cmd.insert(0,"cd ~")
                # cdlist = []
                # for i in cmd:
                #     if i.startswith("cd"):
                #         cdlist.append(i)
                # for i in range(len(cdlist)):
                #     if i + 1 < 
                #     cmd = cmd[cdlist[i]:cdlist[i+1]]
                #     with c.run(cmd[0]) :
                #         for i in cmd[1:]:
                #             print("=" * 30, host, ": ", i, "start", "=" * 30)
                cmdlist = "&&".join(cmd)
                result = c.run(cmdlist)
                if not result.ok :
                    error = "On %s: %s" % (host, result.stderr)
                    print(error)
                    exit(-1)
                print("=" * 30, host, ": ", cmdlist, "finish", "=" * 30)
        finally:
            print("=" * 30, host, " has finished", "=" * 30)

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
