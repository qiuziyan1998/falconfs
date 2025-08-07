# Remote test

Set ALL_SERVERS to actual remote test nodes in distribute.sh, kill_test.sh, remote_run.sh. Enable ssh connection among all nodes, and append the public key of the client node running remote_run.sh to authorized_keys of the nodes in ALL_SERVERS. Modify parameters in remote_run.sh to actual values in used.

``` bash
bash remote-run.sh
```

The IOPS throughput will be printed.

# Local test

Modify parameters in local_run.sh to actual values in used.

``` bash
bash local-run.sh
```

The IOPS throughput will be printed.