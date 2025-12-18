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
    logger_ = logging.getLogger("logger")

    falcon_cm = FalconCM(is_cn=True)
    falcon_cm.connect_zk()
    falcon_cm.init_sys()
    is_leader = falcon_cm.leader_select()
    if is_leader:
        logger_.info("This node is cn leader")
        # become leader by preemption, should manually start some leader services
        if not falcon_cm.is_sys_ready():
            # init, init_filesystem will update cn table later
            # leader should watch_leader_and_candidates for later election (any need?)
            falcon_cm.watch_leader_and_candidates()

            # wait for all nodes' falcon_cm.init_sys() done
            falcon_cm.monitor_nodes()
            # create cns' hostNodes and dns' falcon_clusters/dn0,1,2...
            falcon_cm.build_cluster()

            # wait until all replicas inited by write_replica()
            falcon_cm.wait_until_replicas_nodes_ready()
            # leader should watch_replicas_and_update_cn_table, no need to flush, init_filesystem will flush leader and followers
            falcon_cm.watch_replicas_and_update_cn_table(False)
            try:
                falcon_cm.init_filesystem()
            except Exception as e:
                logger_.error("Failed to init filesystem: {}".format(e))
                raise e
        # in restart, watch_leader_and_candidates will flush leader, and flush followers in watch_replicas_and_update_cn_table(True)
        falcon_cm.watch_need_supplement()
        falcon_cm.init_ready()
    else:
        logger_.info("This node is cn follower")
        # here follower will watch_leader_and_candidates() for later election
        falcon_cm.write_replica()

    falcon_cm.watch_conn()
    if has_falcon_stor:
        falcon_cm.monitor_store_node()
        time.sleep(30)
        falcon_cm.set_store_cluster_ready()
    falcon_cm.start_watch_replica_need_supplement_thread()


if __name__ == "__main__":
    main()
