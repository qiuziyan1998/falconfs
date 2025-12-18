import os
import time
import logging
from utils import logger
from cm.falcon_cm import *


def main():
    data_dir = os.environ.get("data_dir", "/home/falconMeta/data")
    log_level = os.environ.get("cm_log_level", "INFO")
    has_falcon_stor = bool(os.environ.get("has_falcon_stor", "True"))
    cm_log_data_dir = os.path.join(data_dir, "cmlog")
    logger.logging_init(cm_log_data_dir, log_level)

    falcon_cm = FalconCM(is_cn=False)
    falcon_cm.connect_zk()
    falcon_cm.init_sys()
    is_leader = falcon_cm.leader_select()
    if is_leader:
        # both become leader by preemption, should manually start some leader services
        if not falcon_cm.is_sys_ready():
            # init, init_filesystem will flush cn table later
            # leader can watch_leader_and_candidates for later election (any need?)
            falcon_cm.watch_leader_and_candidates()

            # wait until all replicas inited by write_replica(), to fill local cache
            falcon_cm.wait_until_replicas_nodes_ready()
            # leader should watch_replicas_and_update_cn_table, no need to flush, init_filesystem will flush leader and followers
            falcon_cm.watch_replicas_and_update_cn_table(False)
        # in restart, watch_leader_and_candidates will flush leader, and flush followers in watch_replicas_and_update_cn_table(True)
    else:
        # here follower will watch_leader_and_candidates() for later election
        falcon_cm.write_replica()
    falcon_cm.watch_conn()
    if has_falcon_stor:
        falcon_cm.monitor_store_node()
        time.sleep(30)
        falcon_cm.set_store_cluster_ready()
    falcon_cm.start_watch_replica_thread()


if __name__ == "__main__":
    main()
