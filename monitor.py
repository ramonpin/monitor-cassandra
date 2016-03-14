import re
import time
import json
import signal

from io import FileIO
from elasticsearch import Elasticsearch
from yaml import load, Loader
from paramiko import SSHClient, AutoAddPolicy


# noinspection PyUnusedLocal
def sig_handler(signum, frame):
    """
    Para detener el proceso con CTRL+C
        :param signum: not used
        :param frame: not used
    """
    global running
    print 'Parada solicitada por el usuario...'
    running = False


def to_snakecase(name):
    s0 = re.sub(r'-', r'', name)
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', s0)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def vmstat(machinename, sshcli):
    """
    Gets vmstat metrics for machine
        :param machinename: monitored machine name to add to result dict
        :param sshcli: SSHClient to the monitored machine
        :return: Metrics as a dict
    """
    stdin, stdout, stderr = sshcli.exec_command("vmstat")

    # Discard headers
    stdout.readline()
    stdout.readline()

    # Get data
    data = re.split(r'[ ]+', stdout.readline().strip())
    return dict({
       "type":      "vmstat",
       "machine":   machinename,
       "ts":        time.time(),
       "prs-r":     data[0],
       "prs-b":     data[1],
       "mem-swpd":  data[2],
       "mem-free":  data[3],
       "mem-buff":  data[4],
       "mem-cache": data[5],
       "swp-si":    data[6],
       "swp-so":    data[7],
       "io-bi":     data[8],
       "io-bo":     data[9],
       "sys-in":    data[10],
       "sys-cs":    data[11],
       "cpu-us":    data[12],
       "cpu-sy":    data[13],
       "cpu-id":    data[14],
       "cpu-wa":    data[15],
       "cpu-st":    data[16]
    })


def free(machinename, sshcli):
    """
    Gets free metrics for machine
        :param machinename: monitored machine name to add to result dict
        :param sshcli: SSHClient to the monitored machine
        :return: Metrics as a dict
    """
    stdin, stdout, stderr = sshcli.exec_command("free")

    # Skip header
    stdout.readline()
    # Get mem data
    mdata = re.split(r'[ ]+', stdout.readline().strip())
    # Skip header
    stdout.readline()
    # Get swap data
    sdata = re.split(r'[ ]+', stdout.readline().strip())

    return dict({
        "type":        "free",
        "machine":     machinename,
        "ts":          time.time(),
        "mem-total":   mdata[1],
        "mem-used":    mdata[2],
        "mem-free":    mdata[3],
        "mem-shared":  mdata[4],
        "mem-buffers": mdata[5],
        "mem-cached":  mdata[6],
        "swp-total":   sdata[1],
        "swp-used":    sdata[2],
        "swp-free":    sdata[3]
    })


def top(machinename, sshcli):
    """
    Gets top metrics for machine
        :param machinename: monitored machine name to add to result dict
        :param sshcli: SSHClient to the monitored machine
        :return: Metrics as a dict
    """
    stdin, stdout, stderr = sshcli.exec_command("top -b -n 1 | head -3")
    # Skip header
    stdout.readline()

    # tasks/cpu data
    tdata = re.split(r'[ ]+', stdout.readline().strip())
    cdata = re.split(r'[ ]+', stdout.readline().strip())

    return dict({
        "type":       "top",
        "machine":    machinename,
        "ts":         time.time(),
        "tsk-total":  tdata[1],
        "tsk-run":    tdata[3],
        "tsk-sleep":  tdata[5],
        "tsk-stop":   tdata[7],
        "tsk-zombie": tdata[9],
        "cpu-user":   re.sub(r'%[a-z]+,', '', cdata[1]),
        "cpu-sys":    re.sub(r'%[a-z]+,', '', cdata[2]),
        "cpu-nice":   re.sub(r'%[a-z]+,', '', cdata[3]),
        "cpu-idle":   re.sub(r'%[a-z]+,', '', cdata[4]),
        "cpu-wait":   re.sub(r'%[a-z]+,', '', cdata[5])
    })


def disk(machinename, sshcli):
    """
    Gets disk metrics for machine
        :param machinename: monitored machine name to add to result dict
        :param sshcli: SSHClient to the monitored machine
        :return: Metrics as a dict
    """
    stdin, stdout, stderr = sshcli.exec_command("vmstat -d -n")
    # Skip header
    stdout.readline()
    stdout.readline()

    disks = []
    line = stdout.readline()
    while line:
        data = re.split(r'[ ]+', line.strip())
        if not re.match(r'ram|loop', data[0]):
            disks.append({data[0]: data[1:11]})
        line = stdout.readline()

    return dict({
        "type":       "disk",
        "machine":    machinename,
        "ts":         time.time(),
        "disks":      disks
    })


def nt_gcstats(machinename, sshcli):
    """
    Gets nodetool gcstats metrics for machine
        :param machinename: monitored machine name to add to result dict
        :param sshcli: SSHClient to the monitored machine
        :return: Metrics as a dict
    """
    stdin, stdout, stderr = sshcli.exec_command("nodetool gcstats")

    # Skip header
    stdout.readline()
    stdout.readline()

    # garbage collector data
    data = re.split(r'[ ]+', stdout.readline().strip())

    return dict({
        "type":             "gcstats",
        "machine":          machinename,
        "ts":               time.time(),
        "interval":         data[0],
        "max-gc-elapsed":   data[1],
        "total-gc-elapsed": data[2],
        "stdev-gc-elapsed": data[3],
        "gc-reclaimed":     data[4],
        "collections":      data[5],
        "direct-memory":    data[6]
    })


def nt_tpstats(machinename, sshcli):
    """
    Gets nodetool tpstats metrics for machine
        :param machinename: monitored machine name to add to result dict
        :param sshcli: SSHClient to the monitored machine
        :return: Metrics as a dict
    """
    stdin, stdout, stderr = sshcli.exec_command("nodetool tpstats")

    # Skip header
    stdout.readline()
    stdout.readline()

    # dict to collect all metrics
    metrics = dict({
        "type":    "tpstats",
        "machine": machinename,
        "ts":      time.time(),
    })

    # process each metric
    for i in range(0, 20):
        data = re.split(r'[ ]+', stdout.readline().strip())
        metrics.update({
            to_snakecase(data[0]): map(int, data[1:6])
        })

    return metrics


def connections(conf):
    new_conns = dict()
    for srv in conf['servers']:
        sshcli = SSHClient()
        sshcli.load_system_host_keys()
        sshcli.set_missing_host_key_policy(AutoAddPolicy())
        sshcli.connect(srv, port=conf['ssh']['port'], username=conf['ssh']['user'], password=conf['ssh']['password'])
        new_conns.update({srv: sshcli})

    return new_conns


def index_data(conf, esins, body):
    esins.index(index=conf['elastic']['index'], doc_type=body['type'], body=json.dumps(body))

# ###################################################################################
# # START                                                                          ##
# ###################################################################################
config = load(FileIO('./connection.yml', 'r'), Loader)
conns = connections(config)
es = Elasticsearch(config['elastic']['hosts'], port=config['elastic']['port'])

print "Monitor en ejecucion..."

signal.signal(signal.SIGINT, sig_handler)
running = True
while running:

    for server in config['servers']:
        ssh = conns.get(server)
        index_data(config, es, vmstat(server, ssh))
        index_data(config, es, free(server, ssh))
        index_data(config, es, top(server, ssh))
        index_data(config, es, disk(server, ssh))
        index_data(config, es, nt_gcstats(server, ssh))
        index_data(config, es, nt_tpstats(server, ssh))

    time.sleep(config['sleep'])
