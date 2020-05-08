from __future__ import print_function

import tensorflow as tf
import sys
import time
import cdsw_tensorflow_utils
import os

n_workers = 2
n_ps = 1

#curdir = os.path.dirname(os.path.abspath(__file__))
cluster_spec, session_addr = cdsw_tensorflow_utils.run_cluster(n_workers=n_workers, \
   n_ps=n_ps, \
   cpu=0.5, \
   memory=2, \
   worker_script="distributed_mnist_worker_script")

#cdsw_tensorflow_utils.tensorboard('./distributed-mnist')