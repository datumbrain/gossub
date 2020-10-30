package gossub

import "testing"

func Test(t *testing.T) {
	var sl *SparkLauncher
	sl = &SparkLauncher{}
	sl = sl.SetAppName("").SetAppResource("").SetMainClass("")
	_, _ = sl.Launch()
}
