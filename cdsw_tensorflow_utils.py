import cdsw
import os, wait, tempfile, time, json, IPython, subprocess

tf_port = 2323

# Clean up the blank proxy environmental variables,
# which confuse tensorflow.
for thing in ['http_proxy', 'HTTP_PROXY', 'https_proxy', 'https_proxy', 'HTTPS_PROXY', 'no_proxy', 'NO_PROXY', 'all_proxy', 'ALL_PROXY', 'socks_proxy', 'SOCKS_PROXY']:
  if thing in os.environ and os.environ[thing] == '':
    del os.environ[thing]

def tensorboard(fname):
  url = "http://" + os.environ["CDSW_ENGINE_ID"] + os.environ["CDSW_DOMAIN"]
  tb = "/home/cdsw/.local/bin/tensorboard"
  FNULL = open(os.devnull, 'w')
  proc = subprocess.Popen([tb, "--logdir=%s" % fname, "--host=127.0.0.1", "--port=%s" % os.environ["CDSW_APP_PORT"]], stdout=FNULL, stderr=FNULL)
  wait.tcp.open(int(os.environ["CDSW_APP_PORT"]), host="127.0.0.1")
  return url, proc.pid    
    
def tensorflow_worker_code(fname, job_name, worker_script):
  if job_name != "worker" and job_name != "ps":
    raise ValueError("job_name must be 'worker' or 'ps'")
  
  if worker_script is None:
    worker_script_import = ""
  else:
    worker_script_import = "import %s" % worker_script
  
  out = """
import os, time, json, wait

__worker_script_import__

# Clean up the blank proxy environmental variables,
# which confuse tensorflow.
for thing in ['http_proxy', 'HTTP_PROXY', 'https_proxy', 'https_proxy', 'HTTPS_PROXY', 'no_proxy', 'NO_PROXY', 'all_proxy', 'ALL_PROXY', 'socks_proxy', 'SOCKS_PROXY']:
    if thing in os.environ and os.environ[thing] == '':
        del os.environ[thing]

import tensorflow as tf

# Wait for master to tell me the cluster spec.
while True:  
    if os.path.exists("__fname__/cluster.json"):
        break
    else:
        time.sleep(0.1)

clusterSpec = json.loads(open("__fname__/cluster.json").read())
print("Got cluster spec")
print(clusterSpec)
mySpec = "%s:__tf_port__" % os.environ["CDSW_IP_ADDRESS"]
task_index = clusterSpec["__job_name__"].index(mySpec)

cluster = tf.train.ClusterSpec(clusterSpec)
server = tf.train.Server(cluster, job_name="__job_name__", task_index=task_index)    
    """\
      .replace("__fname__", fname)\
      .replace("__job_name__", job_name)\
      .replace("__worker_script_import__", worker_script_import)\
      .replace("__tf_port__", str(tf_port))

  if job_name == "ps" or worker_script is None:
    out += """
server.start()
server.join()
    """
  else:
    out += """
__worker_script__.run(cluster, server, task_index)
  """.replace("__worker_script__", worker_script)
  return out
    
def run_cluster(n_workers, n_ps, cpu, memory, nvidia_gpu=0, worker_script=None, timeout_seconds=60):
  try:
    os.mkdir("/home/cdsw/.tmp", mode=755)
  except:
    pass
  fname = tempfile.mkdtemp(prefix="/home/cdsw/.tmp/clusterspec")

  worker_code=tensorflow_worker_code(fname, "worker", worker_script)
  workers = cdsw.launch_workers(n_workers, cpu=cpu, memory=memory, nvidia_gpu=nvidia_gpu, code=worker_code)
  worker_ids = [worker["id"] for worker in workers]
  if n_ps > 0:
    ps_code=tensorflow_worker_code(fname, "ps", None)
    parameter_servers = cdsw.launch_workers(n_ps, cpu=cpu, memory=memory, code=ps_code)
    ps_ids = [ps["id"] for ps in parameter_servers]
  else:
    parameter_servers = []
    ps_ids = []

  # Get the IP addresses of the workers. First, wait for them all to run
  running_workers = cdsw.await_workers(worker_ids, wait_for_completion=False, timeout_seconds=timeout_seconds)
  if running_workers["failures"]:
    raise RuntimeError("Some workers failed to run")

  # Then extract the IP's from the dictionary describing them.
  worker_ips = [worker["ip_address"] for worker in running_workers["workers"]]

  # Get the IP addresses of the parameter servers, if any
  ps_ips = []
  if n_ps > 0:
    running_ps = cdsw.await_workers(ps_ids, wait_for_completion=False, timeout_seconds=timeout_seconds)
    if running_ps["failures"]:
      raise RuntimeError("Some parameter servers failed to run")

    ps_ips = [ps["ip_address"] for ps in running_ps["workers"]]

  cspec = {
    "worker": [ip + (":%d" % tf_port)for ip in worker_ips],
    "ps": [ip + (":%d" % tf_port) for ip in ps_ips]  
  }
  tmpf = fname + "/cluster.json.tmp"
  f = open(tmpf, 'w')
  f.write(json.dumps(cspec))
  f.flush()
  os.fsync(f.fileno()) 
  f.close()
  os.rename(tmpf, fname + "/cluster.json")
  
  if worker_script is not None:
    # If a script has been provided for the Tensorflow workers,
    # wait for them all to exit.
    cdsw.await_workers(worker_ids, wait_for_completion=True)
    cdsw.stop_workers(ps_ids)
    return None, None
  else:
    # If no script has been provided, wait for the TensorFlow
    # cluster to come up, then return a handle to the lead worker
    # so the user can create a TensorFlow session.
    
    # Wait for workers to be up
    for ip in worker_ips:
      wait.tcp.open(tf_port, host=ip)

    for ip in ps_ips:
      wait.tcp.open(tf_port, host=ip)

    return cspec, "grpc://%s:%d" % (worker_ips[0], tf_port)
  import cdsw
import os, wait, tempfile, time, json, IPython, subprocess

tf_port = 2323

# Clean up the blank proxy environmental variables,
# which confuse tensorflow.
for thing in ['http_proxy', 'HTTP_PROXY', 'https_proxy', 'https_proxy', 'HTTPS_PROXY', 'no_proxy', 'NO_PROXY', 'all_proxy', 'ALL_PROXY', 'socks_proxy', 'SOCKS_PROXY']:
  if thing in os.environ and os.environ[thing] == '':
    del os.environ[thing]

def tensorboard(fname):
  url = "http://" + os.environ["CDSW_ENGINE_ID"] + os.environ["CDSW_DOMAIN"]
  tb = "/home/cdsw/.local/bin/tensorboard"
  FNULL = open(os.devnull, 'w')
  proc = subprocess.Popen([tb, "--logdir=%s" % fname, "--host=127.0.0.1", "--port=%s" % os.environ["CDSW_APP_PORT"]], stdout=FNULL, stderr=FNULL)
  wait.tcp.open(int(os.environ["CDSW_APP_PORT"]), host="127.0.0.1")
  return url, proc.pid    
    
def tensorflow_worker_code(fname, job_name, worker_script):
  if job_name != "worker" and job_name != "ps":
    raise ValueError("job_name must be 'worker' or 'ps'")
  
  if worker_script is None:
    worker_script_import = ""
  else:
    worker_script_import = "import %s" % worker_script
  
  out = """
import os, time, json, wait

__worker_script_import__

# Clean up the blank proxy environmental variables,
# which confuse tensorflow.
for thing in ['http_proxy', 'HTTP_PROXY', 'https_proxy', 'https_proxy', 'HTTPS_PROXY', 'no_proxy', 'NO_PROXY', 'all_proxy', 'ALL_PROXY', 'socks_proxy', 'SOCKS_PROXY']:
    if thing in os.environ and os.environ[thing] == '':
        del os.environ[thing]

import tensorflow as tf

# Wait for master to tell me the cluster spec.
while True:  
    if os.path.exists("__fname__/cluster.json"):
        break
    else:
        time.sleep(0.1)

clusterSpec = json.loads(open("__fname__/cluster.json").read())
print("Got cluster spec")
print(clusterSpec)
mySpec = "%s:__tf_port__" % os.environ["CDSW_IP_ADDRESS"]
task_index = clusterSpec["__job_name__"].index(mySpec)

cluster = tf.train.ClusterSpec(clusterSpec)
server = tf.train.Server(cluster, job_name="__job_name__", task_index=task_index)    
    """\
      .replace("__fname__", fname)\
      .replace("__job_name__", job_name)\
      .replace("__worker_script_import__", worker_script_import)\
      .replace("__tf_port__", str(tf_port))

  if job_name == "ps" or worker_script is None:
    out += """
server.start()
server.join()
    """
  else:
    out += """
__worker_script__.run(cluster, server, task_index)
  """.replace("__worker_script__", worker_script)
  return out
    
def run_cluster(n_workers, n_ps, cpu, memory, nvidia_gpu=0, worker_script=None, timeout_seconds=60):
  try:
    os.mkdir("/home/cdsw/.tmp", mode=755)
  except:
    pass
  fname = tempfile.mkdtemp(prefix="/home/cdsw/.tmp/clusterspec")

  worker_code=tensorflow_worker_code(fname, "worker", worker_script)
  workers = cdsw.launch_workers(n_workers, cpu=cpu, memory=memory, nvidia_gpu=nvidia_gpu, code=worker_code)
  worker_ids = [worker["id"] for worker in workers]
  if n_ps > 0:
    ps_code=tensorflow_worker_code(fname, "ps", None)
    parameter_servers = cdsw.launch_workers(n_ps, cpu=cpu, memory=memory, code=ps_code)
    ps_ids = [ps["id"] for ps in parameter_servers]
  else:
    parameter_servers = []
    ps_ids = []

  # Get the IP addresses of the workers. First, wait for them all to run
  running_workers = cdsw.await_workers(worker_ids, wait_for_completion=False, timeout_seconds=timeout_seconds)
  if running_workers["failures"]:
    raise RuntimeError("Some workers failed to run")

  # Then extract the IP's from the dictionary describing them.
  worker_ips = [worker["ip_address"] for worker in running_workers["workers"]]

  # Get the IP addresses of the parameter servers, if any
  ps_ips = []
  if n_ps > 0:
    running_ps = cdsw.await_workers(ps_ids, wait_for_completion=False, timeout_seconds=timeout_seconds)
    if running_ps["failures"]:
      raise RuntimeError("Some parameter servers failed to run")

    ps_ips = [ps["ip_address"] for ps in running_ps["workers"]]

  cspec = {
    "worker": [ip + (":%d" % tf_port)for ip in worker_ips],
    "ps": [ip + (":%d" % tf_port) for ip in ps_ips]  
  }
  tmpf = fname + "/cluster.json.tmp"
  f = open(tmpf, 'w')
  f.write(json.dumps(cspec))
  f.flush()
  os.fsync(f.fileno()) 
  f.close()
  os.rename(tmpf, fname + "/cluster.json")
  
  if worker_script is not None:
    # If a script has been provided for the Tensorflow workers,
    # wait for them all to exit.
    cdsw.await_workers(worker_ids, wait_for_completion=True)
    cdsw.stop_workers(*ps_ids)
    return None, None
  else:
    # If no script has been provided, wait for the TensorFlow
    # cluster to come up, then return a handle to the lead worker
    # so the user can create a TensorFlow session.
    
    # Wait for workers to be up
    for ip in worker_ips:
      wait.tcp.open(tf_port, host=ip)

    for ip in ps_ips:
      wait.tcp.open(tf_port, host=ip)

    return cspec, "grpc://%s:%d" % (worker_ips[0], tf_port)
  