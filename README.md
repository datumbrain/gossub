# gossub (Go `spark-submit` wrapper)

Trigger spark-submit in Golang. A Go implementation of `org.apache.spark.launcher.SparkLauncher`.

## Usage

```go
package main

import (
	"os"

	"github.com/datumbrain/gossub"
)

func main() {
	sl := gossub.SparkLauncher{
		AppName:     "my-new-app",
		AppResource: "test",
		MainClass:   "org.apache.spark.examples.SparkPi",
		SparkHome:   "/usr/local/spark",
		Jar:         "original-spark-examples_2.12-3.1.0-SNAPSHOT.jar",
	}

	sl.Init()
	sl.RedirectError(os.Stderr)
	sl.RedirectOutput(os.Stdout)
	sl.Launch()
}
```

## Author

[Fahad Siddiqui](https://github.com/fahadsiddiqui)

## License 

MIT
