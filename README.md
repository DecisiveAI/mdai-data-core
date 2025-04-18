# mdai-data-core

## Getting Started
### Importing mdai-operator module from private repo
1. make sure the following env variable is set
```shell
export GOPRIVATE=github.com/decisiveai/*
```

2. Add the following section to your git client config:
```shell
[url "ssh://git@github.com/"]
	insteadOf = https://github.com/
```