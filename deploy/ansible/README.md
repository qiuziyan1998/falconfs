# FalconFS Cluster Test Setup Guide

Usage:

0. Install ansible with apt.

```
apt update && apt install -y ansible sshpass
```

1. Create your user on all nodes. You can do this manually or by ansible with your own playbook.


```
useradd -m -s /bin/bash falcon
passwd falcon
# input your password here
# repeat the password
usermod -aG sudo falcon # add sudo for falcon
```

2. Set up SSH key-based authentication for passwordless login under user falcon

3. Prepare working directory

```bash
mkdir -p ~/code/ansible
cd ~/code/ansible
wget https://raw.githubusercontent.com/falcon-infra/falconfs/main/deploy/ansible/inventory
wget https://raw.githubusercontent.com/falcon-infra/falconfs/main/deploy/ansible/falcontest.yml
wget https://raw.githubusercontent.com/falcon-infra/falconfs/main/deploy/ansible/install-ubuntu24.04.sh
wget https://raw.githubusercontent.com/falcon-infra/falconfs/main/deploy/ansible/install-ubuntu22.04.sh # if ubuntu22.04
```

4. Create .ansible.cfg at your home `~/.ansible.cfg`. The content can be like:

```
[defaults]
inventory = /home/$USER/code/ansible/inventory
log_path = /home/$USER/code/ansible/ansible.log
```

5. Set value in inventory according to the annotation.

- change ips in `[falconcn] [falcondn] [falconclient]`
- fill ansible_become_password

6. Install dependencies.

```
ansible-playbook falcontest.yml --tags install-deps
```

7. Clone code and Build.

```
ansible-playbook falcontest.yml --tags build
```

8. Start.

```
ansible-playbook falcontest.yml --tags start
```

9. Stop.

```
ansible-playbook falcontest.yml --tags stop
```

