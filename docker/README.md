## FalconFS dev machine

- ubuntu 22.04
- FalconFS dev machine version `0.1.1`

## build

```bash
docker build -f docker/ubuntu22.04-dockerfile -t ghcr.io/falcon-infra/falconfs-dev .
```

## test

```bash
docker run -it --rm -v `pwd`/..:/root/code ghcr.io/falcon-infra/falconfs-dev /bin/zsh
```

## deploy

```
docker tag ghcr.io/falcon-infra/falconfs-dev ghcr.io/falcon-infra/falconfs-dev:0.1.1
docker push ghcr.io/falcon-infra/falconfs-dev:0.1.1
```
