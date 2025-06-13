# mdai-data-core
[![Chores](https://github.com/DecisiveAI/mdai-data-core/actions/workflows/chores.yml/badge.svg)](https://github.com/DecisiveAI/mdai-data-core/actions/workflows/chores.yml)

## Overview
`mdai-data-core` is a Go library designed for efficient and structured interaction with Valkey storage.   

It simplifies:
-	**Variable Access**: Conveniently encapsulates and manages variables stored in Valkey.
-	**Audit Management**: Provides robust auditing capabilities for operations performed on Valkey variables and other critical MDAI operations.
-	**Handlers Integration**: Offers streamlined handlers interface for interacting directly with Valkey-stored data.

## Installation
```shell
go get github.com/decisiveai/mdai-data-core
```

## Usage
Basic usage example:

```go
package main

import (
	"context"

	datacore "github.com/decisiveai/mdai-data-core/variables"
)

func main() {
	// initialize your valkeyClient (valkey-go), provide logger
	client := datacore .NewValkeyAdapter(valKeyClient, zapLogger)
	value, found, err := client.GetString(context.TODO(), "your_variable_name", "hub_name")
	// proceed with error hadling and the rest of your logic
}
```
