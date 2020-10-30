package gossub

import (
	"io"
	"os"
	"os/exec"
	"path"
)

type Launcher interface {
	SetConfig(k, v string) *SparkLauncher
	SetJavaHome(javaHome string) *SparkLauncher
	SetSparkHome(sparkHome string) *SparkLauncher
	Directory(file os.File) *SparkLauncher
	RedirectError(w *io.Writer) *SparkLauncher
	RedirectOutput(w *io.Writer) *SparkLauncher
	RedirectErrorToFile(file *os.File) *SparkLauncher
	RedirectOutputToFile(file *os.File) *SparkLauncher
	SetConf(k, v string) *SparkLauncher
	SetAppName(appName string) *SparkLauncher
	SetMaster(master string) *SparkLauncher
	SetDeployMode(deployMode string) *SparkLauncher
	SetAppResource(deployMode string) *SparkLauncher
	SetMainClass(mainClass string) *SparkLauncher
	AddSparkArgs(arg string) *SparkLauncher
	AddJar(jar string) *SparkLauncher
	AddPyFile(file string) *SparkLauncher
	SetVerbose(verbose bool) *SparkLauncher
	Launch() *os.Process
}

type SparkLauncher struct {
	cmd         *exec.Cmd
	conf        map[string]string
	javaHome    string
	sparkHome   string
	pwd         string
	appName     string
	master      string
	deployMode  string
	appResource string
	mainClass   string
	sparkArgs   []string
	jar         string
	pyFile      string
	verbose     bool
}

func (sl *SparkLauncher) redirectOutput(w *io.Writer) *SparkLauncher {
	if w == nil {
		panic("nil *io.Writer passed")
	}

	sl.cmd.Stdout = *w
	return sl
}

func (sl *SparkLauncher) redirectError(w *io.Writer) *SparkLauncher {
	if w == nil {
		panic("nil *io.Writer passed")
	}

	sl.cmd.Stderr = *w
	return sl
}

func (sl *SparkLauncher) RedirectError(w *io.Writer) *SparkLauncher {
	return sl.redirectError(w)
}

func (sl *SparkLauncher) RedirectOutput(w *io.Writer) *SparkLauncher {
	return sl.redirectOutput(w)
}

func (sl *SparkLauncher) RedirectOutputToFile(file *os.File) *SparkLauncher {
	w := io.MultiWriter(os.Stdout, file)
	return sl.redirectOutput(&w)
}

func (sl *SparkLauncher) RedirectErrorToFile(file *os.File) *SparkLauncher {
	w := io.MultiWriter(os.Stderr, file)
	return sl.redirectOutput(&w)
}

func (sl *SparkLauncher) SetConf(k, v string) *SparkLauncher {
	sl.conf[k] = v
	return sl
}

func (sl *SparkLauncher) SetAppName(appName string) *SparkLauncher {
	sl.appName = appName
	return sl
}

func (sl *SparkLauncher) SetMaster(master string) *SparkLauncher {
	sl.master = master
	return sl
}

func (sl *SparkLauncher) SetDeployMode(deployMode string) *SparkLauncher {
	sl.deployMode = deployMode
	return sl
}

func (sl *SparkLauncher) SetAppResource(appResource string) *SparkLauncher {
	sl.appResource = appResource
	return sl
}

func (sl *SparkLauncher) SetMainClass(mainClass string) *SparkLauncher {
	sl.mainClass = mainClass
	return sl
}

func (sl *SparkLauncher) AddSparkArgs(arg string) *SparkLauncher {
	sl.sparkArgs = append(sl.sparkArgs, arg)
	return sl
}

func (sl *SparkLauncher) AddJar(jar string) *SparkLauncher {
	sl.jar = jar
	return sl
}

func (sl *SparkLauncher) AddPyFile(file string) *SparkLauncher {
	sl.pyFile = file
	return sl
}

func (sl *SparkLauncher) SetVerbose(verbose bool) *SparkLauncher {
	sl.verbose = verbose
	return sl
}

func (sl *SparkLauncher) findSparkSubmit() string {
	script := "spark-submit"
	return path.Join(sl.sparkHome, "bin", script)
}

func (sl *SparkLauncher) Launch() (*os.Process, error) {
	var err error

	// TODO: build proper shell command for spark-submit
	sl.cmd = exec.Command(sl.findSparkSubmit())
	err = sl.cmd.Run()

	return sl.cmd.Process, err
}
