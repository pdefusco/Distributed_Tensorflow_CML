# CLOUDERA BLOG-SAMPLE-CODE 1.0
# (c) 2020 - 2020 Cloudera, Inc. All rights reserved.  
# This code is provided to you pursuant to the Apache License 2.0,
#
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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