#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
脏数据回放入口：把 job.setting.dirtyRecord 落盘的 jsonl 脏数据重新送入 writer。
用法:
  python datax-replay.py --sink <脏数据落盘目录> --job <原始job.json>
                         [--force] [--allow-manifest-missing]
                         [--re-dirty-sink <回放再脏数据输出根目录>]
                         [-j "<JVM参数>"]
"""

import os
import platform
import signal
import subprocess
import sys
from optparse import OptionParser
from string import Template

DATAX_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if platform.system() == 'Windows':
    CLASS_PATH = ("%s/lib/*") % (DATAX_HOME)
else:
    CLASS_PATH = ("%s/lib/*:.") % (DATAX_HOME)
LOGBACK_FILE = ("%s/conf/logback.xml") % (DATAX_HOME)
DEFAULT_JVM = "-Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s/log" % (DATAX_HOME)
DEFAULT_PROPERTY_CONF = "-Dfile.encoding=UTF-8 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener -Djava.security.egd=file:///dev/urandom -Ddatax.home=%s -Dlogback.configurationFile=%s" % (
    DATAX_HOME, LOGBACK_FILE)
REPLAY_ENGINE = "com.alibaba.datax.core.replay.DirtyRecordReplayEngine"
REPLAY_COMMAND = "java -server ${jvm} %s -classpath %s  ${params} %s ${replayArgs}" % (
    DEFAULT_PROPERTY_CONF, CLASS_PATH, REPLAY_ENGINE)

RET_STATE = {"KILL": 143, "FAIL": -1, "OK": 0}

child_process = None


def suicide(signum, e):
    global child_process
    sys.stderr.write("[Error] datax-replay receive unexpected signal %d\n" % signum)
    if child_process:
        child_process.send_signal(signal.SIGQUIT)
        sys.exit(RET_STATE["KILL"])


def get_option_parser():
    parser = OptionParser(usage="usage: %prog --sink <dir> --job <job.json> [options]")
    parser.add_option("", "--sink", dest="sink", action="store",
                      help="脏数据落盘目录（包含 manifest.json 与 *.jsonl），必填")
    parser.add_option("", "--job", dest="job", action="store",
                      help="原始同步任务的 job json，回放只使用其 writer 配置，必填")
    parser.add_option("", "--force", dest="force", action="store_true", default=False,
                      help="存在 _REPLAYED 标记时仍强制重复回放")
    parser.add_option("", "--allow-manifest-missing", dest="allowManifestMissing",
                      action="store_true", default=False,
                      help="manifest.json 缺失时允许降级按 jsonl 直接回放（跳过校验和）")
    parser.add_option("", "--re-dirty-sink", dest="reDirtySink", action="store",
                      help="回放中再次变脏记录的输出根目录，默认 <sink>/replay-redirty-<时间戳>")
    parser.add_option("-j", "--jvm", metavar="<jvm parameters>", dest="jvmParameters",
                      action="store", default=DEFAULT_JVM, help="设置JVM参数")
    return parser


def build_command(options):
    args = []
    args.append("--sink")
    args.append(os.path.abspath(options.sink))
    args.append("--job")
    args.append(os.path.abspath(options.job))
    if options.force:
        args.append("--force")
    if options.allowManifestMissing:
        args.append("--allow-manifest-missing")
    if options.reDirtySink:
        args.append("--re-dirty-sink")
        args.append(os.path.abspath(options.reDirtySink))

    command_map = {
        "jvm": options.jvmParameters,
        "params": "-Dlog.file.name=datax-replay",
        "replayArgs": " ".join(args),
    }
    return Template(REPLAY_COMMAND).substitute(**command_map)


if __name__ == "__main__":
    parser = get_option_parser()
    options, args = parser.parse_args(sys.argv[1:])
    if not options.sink or not options.job:
        parser.print_help()
        sys.exit(RET_STATE["FAIL"])

    signal.signal(2, suicide)
    signal.signal(15, suicide)

    start_command = build_command(options)
    child_process = subprocess.Popen(start_command, shell=True)
    (stdout, stderr) = child_process.communicate()
    sys.exit(child_process.returncode)
