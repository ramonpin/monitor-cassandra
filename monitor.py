import re
import time
from io import FileIO
from paramiko import SSHClient, AutoAddPolicy
from yaml import load, Loader


def vmstat(machinename, sshcli):
    """
    Gets vmstat metrics for machine
        :param machinename: monitored machine name to add to result dict
        :param sshcli: SSHClient to the monitored machine
    """
    stdin, stdout, stderr = sshcli.exec_command("vmstat")

    # Discard headers
    stdout.readline()
    stdout.readline()

    # Get data
    data = re.split("[ ]+", stdout.readline().strip())
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
    """
    stdin, stdout, stderr = sshcli.exec_command("free")

    # Skip header
    stdout.readline()
    # Get mem data
    mdata = re.split("[ ]+", stdout.readline().strip())
    # Skip header
    stdout.readline()
    # Get swap data
    sdata = re.split("[ ]+", stdout.readline().strip())

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
    """
    stdin, stdout, stderr = sshcli.exec_command("top -b -n 1 | head -3")
    # Skip header
    stdout.readline()

    # tasks/cpu data
    tdata = re.split("[ ]+", stdout.readline().strip())
    cdata = re.split("[ ]+", stdout.readline().strip())

    return dict({
        "type":       "top",
        "machine":    machinename,
        "ts":         time.time(),
        "tsk-total":  tdata[1],
        "tsk-run":    tdata[3],
        "tsk-sleep":  tdata[5],
        "tsk-stop":   tdata[7],
        "tsk-zombie": tdata[9],
        "cpu-user":   re.sub("%[a-z]+,", "", cdata[1]),
        "cpu-sys":    re.sub("%[a-z]+,", "", cdata[2]),
        "cpu-nice":   re.sub("%[a-z]+,", "", cdata[3]),
        "cpu-idle":   re.sub("%[a-z]+,", "", cdata[4]),
        "cpu-wait":   re.sub("%[a-z]+,", "", cdata[5])
    })


def nt_gcstats(machinename, sshcli):
    """
    Gets nodetool gcstats metrics for machine
        :param machinename: monitored machine name to add to result dict
        :param sshcli: SSHClient to the monitored machine
    """
    stdin, stdout, stderr = sshcli.exec_command("nodetool gcstats")

    # Skip header
    stdout.readline()

    # garbage collector data
    data = re.split("[ ]+", stdout.readline().strip())

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


def connections(conf):
    new_conns = dict()
    for srv in conf['servers']:
        sshcli = SSHClient()
        sshcli.load_system_host_keys()
        sshcli.set_missing_host_key_policy(AutoAddPolicy())
        sshcli.connect(srv, conf['port'], username=conf['user'], password=conf['password'])
        new_conns.update({srv: sshcli})

    return new_conns

# ###################################################################################
# # START                                                                          ##
# ###################################################################################
config = load(FileIO("./connection.yml", "r"), Loader)
conns = connections(config)

while True:

    for server in config['servers']:
        ssh = conns.get(server)
        print vmstat(server, ssh)
        print free(server,   ssh)
        print top(server,    ssh)
        print nt_gcstats(server,    ssh)
        time.sleep(2)
