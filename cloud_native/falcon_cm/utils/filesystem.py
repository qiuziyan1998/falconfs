import psycopg2
import logging
import time


def split_ip_port(leader_info):
    split_str = leader_info.split(":")
    return split_str[0], split_str[1]


def init_filesystem(leader_infos, follower_infos, user, replica_server_num):
    logger = logging.getLogger("logger")
    cluster_names = list(leader_infos.keys())
    server_num = len(cluster_names)

    create_extension = "CREATE EXTENSION falcon;"
    stat_replication_sql = "SELECT state from pg_stat_replication;"
    # check the leader for replication status
    for i in range(server_num):
        name = cluster_names[i]
        ip, port = split_ip_port(leader_infos[name])
        conn = psycopg2.connect(host=ip, port=port, user=user, database="postgres")
        # check if the replication is ready
        with conn:
            cursor = conn.cursor()
            while replica_server_num > 0:
                cursor.execute(stat_replication_sql)
                res = cursor.fetchall()
                if len(res) >= replica_server_num and res[0][0] == "streaming":
                    logger.info("{}--pg_stat_replication results: {}".format(ip, name))
                    break
                time.sleep(0.5)
            cursor.execute(create_extension)
            conn.commit()
    
    # build foreign_server_table in all leaders' database row by row
    # leader_infos = [leader0, leader1, ...], follower_infos = [[follower0, follower1], [follower0, follower1], ...]

    # merge: leader + followers
    all_nodes_info = []
    for i in range(server_num):
        name = cluster_names[i]
        group_id = 0 if name == "cn" else int(name[2:]) + 1
        base_server_id = group_id * (len(follower_infos[name]) + 1)

        # leader
        leader_ip, leader_port = split_ip_port(leader_infos[name])
        all_nodes_info.append((base_server_id, name, leader_ip, leader_port, group_id, True))

        # follower
        for j, follower_info in enumerate(follower_infos[name], 1):
            follower_ip, follower_port = split_ip_port(follower_info)
            follower_name = f"{name}_f{j}"
            all_nodes_info.append((base_server_id + j, follower_name, follower_ip, follower_port, group_id, False))
    # insert all info to leaders' database
    for i in range(server_num):
        name = cluster_names[i]
        to_group_id = 0 if name == "cn" else int(name[2:]) + 1
        to_ip, to_port = split_ip_port(leader_infos[name])

        logger.info(f"init_filesystem: send sql to leader: {to_ip}:{to_port}")
        conn = psycopg2.connect(host=to_ip, port=to_port, user=user, database="postgres")
        with conn:
            cursor = conn.cursor()
            # insert all primary and backup to each leader's db
            for server_id, name, ip, port, group_id, is_leader in all_nodes_info:
                is_local = "true" if (group_id == to_group_id) else "false"
                insert_server_sql = "SELECT falcon_insert_foreign_server({}, '{}', '{}', {}, {}, '{}', {}, {});".format(
                    server_id, name, ip, port, is_local, user, group_id, is_leader
                )
                logger.info(f"init_filesystem: send sql: {insert_server_sql}")
                cursor.execute(insert_server_sql)
            conn.commit()
        
        logger.info(f"init_filesystem: one leader done -------------------------------------")

    # build shard_table
    shard_count = 100 * (server_num - 1)
    for i in range(server_num):
        ip, port = split_ip_port(leader_infos[cluster_names[i]])
        build_shard_map_sql = "SELECT falcon_build_shard_table({});SELECT falcon_create_distributed_data_table();SELECT falcon_start_background_service();".format(
            shard_count
        )
        conn = psycopg2.connect(host=ip, port=port, user=user, database="postgres")
        with conn:
            cursor = conn.cursor()
            cursor.execute(build_shard_map_sql)
            conn.commit()
    # mkdir the initial '/'
    mkdir_sql = "SELECT * from falcon_plain_mkdir('/');"
    ip, port = split_ip_port(leader_infos["cn"])
    conn = psycopg2.connect(host=ip, port=port, user=user, database="postgres")
    with conn:
        cursor = conn.cursor()
        cursor.execute(mkdir_sql)
        conn.commit()
    logger.info("Init filesystem done")
