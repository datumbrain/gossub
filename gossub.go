package gossub

import (
	"os"
	"os/exec"
	"path"
	"runtime"
)

const (
	SPARK_MASTER                  = "spark.master"
	DEPLOY_MODE                   = "spark.submit.deployMode"
	DRIVER_MEMORY                 = "spark.driver.memory"
	DRIVER_EXTRA_CLASSPATH        = "spark.driver.extraClassPath"
	DRIVER_DEFAULT_JAVA_OPTIONS   = "spark.driver.defaultJavaOptions"
	DRIVER_EXTRA_JAVA_OPTIONS     = "spark.driver.extraJavaOptions"
	DRIVER_EXTRA_LIBRARY_PATH     = "spark.driver.extraLibraryPath"
	EXECUTOR_MEMORY               = "spark.executor.memory"
	EXECUTOR_EXTRA_CLASSPATH      = "spark.executor.extraClassPath"
	EXECUTOR_DEFAULT_JAVA_OPTIONS = "spark.executor.defaultJavaOptions"
	EXECUTOR_EXTRA_JAVA_OPTIONS   = "spark.executor.extraJavaOptions"
	EXECUTOR_EXTRA_LIBRARY_PATH   = "spark.executor.extraLibraryPath"
	EXECUTOR_CORES                = "spark.executor.cores"
	PYSPARK_DRIVER_PYTHON         = "spark.pyspark.driver.python"
	PYSPARK_PYTHON                = "spark.pyspark.python"
	SPARKR_R_SHELL                = "spark.r.shell.command"
	CHILD_PROCESS_LOGGER_NAME     = "spark.launcher.childProcLoggerName"
	NO_RESOURCE                   = "spark-internal"
	CHILD_CONNECTION_TIMEOUT      = "spark.launcher.childConectionTimeout"
)

type Launcher interface {
	SetConfig(k, v string) *SparkLauncher
	Directory(file os.File) *SparkLauncher
	RedirectError(file *os.File)
	RedirectOutput(file *os.File)
	SetConf(k, v string) *SparkLauncher
	AddSparkArgs(arg string) *SparkLauncher
	Launch() *os.Process
}

type SparkLauncher struct {
	cmd       *exec.Cmd
	conf      map[string]string
	sparkArgs []string

	JavaHome    string
	SparkHome   string
	Pwd         string
	AppName     string
	Master      string
	DeployMode  string
	AppResource string
	MainClass   string
	Jar         string
	PyFile      string
	Verbose     bool
}

func (sl *SparkLauncher) Init() {
	sl.cmd = exec.Command(
		sl.findSparkSubmit(),
		"--class",
		sl.MainClass,
		sl.Jar,
	)
}

func (sl *SparkLauncher) RedirectError(file *os.File) {
	sl.cmd.Stderr = file
}

func (sl *SparkLauncher) RedirectOutput(file *os.File) {
	sl.cmd.Stdout = file
}

func (sl *SparkLauncher) SetConf(k, v string) *SparkLauncher {
	sl.conf[k] = v
	return sl
}

func (sl *SparkLauncher) AddSparkArgs(arg string) *SparkLauncher {
	sl.sparkArgs = append(sl.sparkArgs, arg)
	return sl
}

func (sl *SparkLauncher) findSparkSubmit() string {
	script := func() string {
		scr := "spark-submit"
		if runtime.GOOS == "windows" {
			scr += ".cmd"
		}

		return scr
	}()

	return path.Join(sl.SparkHome, "bin", script)
}

func (sl *SparkLauncher) Launch() (*os.Process, error) {
	var err error
	
	err = sl.cmd.Run()

	return sl.cmd.Process, err
}
