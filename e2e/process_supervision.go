// Package e2e verifies Spaniel through its public HTTP and command boundaries.
package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	supervisorEnv     = "SPANIEL_PROCESS_SUPERVISOR"
	livenessFD        = 3
	statusFD          = 4
	defaultGrace      = 5 * time.Second
	defaultHardWait   = 5 * time.Second
	statusReadTimeout = 10 * time.Second
)

type supervisedCommand struct {
	Args []string `json:"args"`
	Dir  string   `json:"dir,omitempty"`
	Env  []string `json:"env,omitempty"`
}

type supervisorOptions struct {
	ExitWithChild bool
	Grace         time.Duration
	HardWait      time.Duration
	MaxRuntime    time.Duration
	Stderr        io.Writer
	Stdout        io.Writer
}

type supervisorConfig struct {
	Commands      []supervisedCommand `json:"commands"`
	ExitWithChild bool                `json:"exitWithChild"`
	Grace         time.Duration       `json:"grace"`
	HardWait      time.Duration       `json:"hardWait"`
	MaxRuntime    time.Duration       `json:"maxRuntime"`
}

type supervisorStatus struct {
	PIDs  []int  `json:"pids,omitempty"`
	Error string `json:"error,omitempty"`
}

type processSupervisor struct {
	command  *exec.Cmd
	done     chan error
	liveness *os.File
	stopOnce sync.Once
}

func runSupervisorIfRequested() {
	if os.Getenv(supervisorEnv) != "1" {
		return
	}
	os.Exit(runSupervisor())
}

//nolint:cyclop,funlen // Startup validates and closes every pipe/process at its acquisition boundary.
func startSupervisor(
	ctx context.Context,
	commands []supervisedCommand,
	options supervisorOptions,
) (*processSupervisor, error) {
	if len(commands) == 0 {
		return nil, errors.New("start process supervisor: commands must not be empty")
	}
	for index, command := range commands {
		if len(command.Args) == 0 || command.Args[0] == "" {
			return nil, fmt.Errorf("start process supervisor: command %d has no executable", index)
		}
	}
	if options.Grace <= 0 {
		options.Grace = defaultGrace
	}
	if options.HardWait <= 0 {
		options.HardWait = defaultHardWait
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve supervisor executable: %w", err)
	}
	livenessRead, livenessWrite, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("create supervisor liveness pipe: %w", err)
	}
	statusRead, statusWrite, err := os.Pipe()
	if err != nil {
		_ = livenessRead.Close()
		_ = livenessWrite.Close()
		return nil, fmt.Errorf("create supervisor status pipe: %w", err)
	}

	command := exec.CommandContext(context.WithoutCancel(ctx), executable)
	command.Env = append(os.Environ(), supervisorEnv+"=1")
	command.ExtraFiles = []*os.File{livenessRead, statusWrite}
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	command.Stdout = options.Stdout
	command.Stderr = options.Stderr
	stdin, err := command.StdinPipe()
	if err != nil {
		closeSupervisorFiles(livenessRead, livenessWrite, statusRead, statusWrite)
		return nil, fmt.Errorf("open supervisor config pipe: %w", err)
	}
	if err := command.Start(); err != nil {
		closeSupervisorFiles(livenessRead, livenessWrite, statusRead, statusWrite)
		return nil, fmt.Errorf("start detached process supervisor: %w", err)
	}
	_ = livenessRead.Close()
	_ = statusWrite.Close()
	encodeErr := json.NewEncoder(stdin).Encode(supervisorConfig{
		Commands: commands, ExitWithChild: options.ExitWithChild, Grace: options.Grace,
		HardWait: options.HardWait, MaxRuntime: options.MaxRuntime,
	})
	closeErr := stdin.Close()
	if encodeErr != nil || closeErr != nil {
		_ = livenessWrite.Close()
		_ = statusRead.Close()
		_ = command.Process.Kill()
		_ = command.Wait()
		return nil, errors.Join(
			supervisorWrap("send supervisor config", encodeErr),
			supervisorWrap("close supervisor config pipe", closeErr),
		)
	}

	statusCtx, cancel := context.WithTimeout(ctx, statusReadTimeout)
	defer cancel()
	started, err := readSupervisorStatus(statusCtx, statusRead)
	_ = statusRead.Close()
	if err != nil || started.Error != "" {
		_ = livenessWrite.Close()
		_ = command.Process.Kill()
		_ = command.Wait()
		if err != nil {
			return nil, fmt.Errorf("read supervisor startup: %w", err)
		}
		return nil, fmt.Errorf("start supervisor children: %s", started.Error)
	}

	group := &processSupervisor{command: command, done: make(chan error, 1), liveness: livenessWrite}
	go func() {
		group.done <- command.Wait()
		close(group.done)
	}()
	return group, nil
}

func (group *processSupervisor) Done() <-chan error {
	return group.done
}

func (group *processSupervisor) signal(value syscall.Signal) error {
	if group.command == nil || group.command.Process == nil {
		return nil
	}
	if err := group.command.Process.Signal(value); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("signal supervisor with %s: %w", value, err)
	}
	return nil
}

func (group *processSupervisor) shutdown(ctx context.Context, value syscall.Signal) error {
	var signalErr error
	group.stopOnce.Do(func() {
		signalErr = group.signal(value)
		if group.liveness != nil {
			signalErr = errors.Join(signalErr, group.liveness.Close())
		}
	})
	select {
	case err := <-group.done:
		return errors.Join(signalErr, acceptableSupervisorExit(err))
	case <-ctx.Done():
		return errors.Join(signalErr, fmt.Errorf("join process supervisor: %w", ctx.Err()))
	}
}

//nolint:cyclop,funlen // Supervisor shutdown converges signals, EOF, timeout, and child exit.
func runSupervisor() int {
	var cfg supervisorConfig
	if err := json.NewDecoder(os.Stdin).Decode(&cfg); err != nil {
		writeSupervisorStatus(supervisorStatus{Error: fmt.Sprintf("decode supervisor config: %v", err)})
		return 1
	}
	liveness := os.NewFile(livenessFD, "supervisor-liveness")
	statusFile := os.NewFile(statusFD, "supervisor-status")
	if liveness == nil || statusFile == nil {
		return 1
	}
	children, err := startSupervisorChildren(cfg.Commands)
	if err != nil {
		if statusErr := json.NewEncoder(statusFile).Encode(supervisorStatus{Error: err.Error()}); statusErr != nil {
			fmt.Fprintf(os.Stderr, "encode supervisor startup error: %v\n", statusErr)
		}
		_ = statusFile.Close()
		return 1
	}
	childExit := make(chan error, len(children))
	allDone := make(chan struct{})
	var waits sync.WaitGroup
	for _, child := range children {
		waits.Go(func() {
			childExit <- child.Wait()
		})
	}
	go func() {
		waits.Wait()
		close(allDone)
	}()
	livenessEOF := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, liveness)
		close(livenessEOF)
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	if err := json.NewEncoder(statusFile).Encode(supervisorStatus{PIDs: supervisorChildPIDs(children)}); err != nil {
		fmt.Fprintf(os.Stderr, "encode supervisor startup status: %v\n", err)
		signalSupervisorChildren(children, syscall.SIGKILL)
		return 1
	}
	_ = statusFile.Close()

	var timer <-chan time.Time
	if cfg.MaxRuntime > 0 {
		deadline := time.NewTimer(cfg.MaxRuntime)
		defer deadline.Stop()
		timer = deadline.C
	}
	exitCode := 0
	shutdownSignal := syscall.SIGTERM
	select {
	case <-livenessEOF:
	case value := <-signals:
		if actual, ok := value.(syscall.Signal); ok {
			shutdownSignal = actual
		}
	case <-timer:
		exitCode = 1
	case err := <-childExit:
		if !cfg.ExitWithChild || err != nil {
			exitCode = 1
		}
	}

	signalSupervisorChildren(children, shutdownSignal)
	grace := cfg.Grace
	if grace <= 0 {
		grace = defaultGrace
	}
	select {
	case <-allDone:
		return exitCode
	case <-time.After(grace):
	}
	signalSupervisorChildren(children, syscall.SIGKILL)
	hardWait := cfg.HardWait
	if hardWait <= 0 {
		hardWait = defaultHardWait
	}
	select {
	case <-allDone:
		return exitCode
	case <-time.After(hardWait):
		return 1
	}
}

func startSupervisorChildren(commands []supervisedCommand) ([]*exec.Cmd, error) {
	children := make([]*exec.Cmd, 0, len(commands))
	for index, spec := range commands {
		// #nosec G204 -- commands are explicit harness inputs, executed only inside the detached supervisor.
		child := exec.CommandContext(context.Background(), spec.Args[0], spec.Args[1:]...)
		child.Dir = spec.Dir
		child.Env = spec.Env
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := child.Start(); err != nil {
			signalSupervisorChildren(children, syscall.SIGKILL)
			return nil, fmt.Errorf("start supervisor command %d %q: %w", index, spec.Args[0], err)
		}
		children = append(children, child)
	}
	return children, nil
}

func signalSupervisorChildren(children []*exec.Cmd, value syscall.Signal) {
	for _, child := range children {
		if child.Process == nil {
			continue
		}
		err := syscall.Kill(-child.Process.Pid, value)
		if err != nil && !errors.Is(err, syscall.ESRCH) {
			fmt.Fprintf(os.Stderr, "signal process group %d with %s: %v\n", child.Process.Pid, value, err)
		}
	}
}

func supervisorChildPIDs(children []*exec.Cmd) []int {
	pids := make([]int, 0, len(children))
	for _, child := range children {
		pids = append(pids, child.Process.Pid)
	}
	return pids
}

func readSupervisorStatus(ctx context.Context, input io.Reader) (supervisorStatus, error) {
	result := make(chan struct {
		status supervisorStatus
		err    error
	}, 1)
	go func() {
		var value supervisorStatus
		err := json.NewDecoder(bufio.NewReader(input)).Decode(&value)
		result <- struct {
			status supervisorStatus
			err    error
		}{status: value, err: err}
	}()
	select {
	case decoded := <-result:
		return decoded.status, decoded.err
	case <-ctx.Done():
		return supervisorStatus{}, fmt.Errorf("read supervisor status: %w", ctx.Err())
	}
}

func writeSupervisorStatus(value supervisorStatus) {
	file := os.NewFile(statusFD, "supervisor-status")
	if file == nil {
		return
	}
	if err := json.NewEncoder(file).Encode(value); err != nil {
		fmt.Fprintf(os.Stderr, "encode supervisor status: %v\n", err)
	}
	_ = file.Close()
}

func closeSupervisorFiles(files ...*os.File) {
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
}

func acceptableSupervisorExit(err error) error {
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		status, ok := exitErr.Sys().(syscall.WaitStatus)
		if ok && (status.Signal() == syscall.SIGINT || status.Signal() == syscall.SIGTERM) {
			return nil
		}
	}
	return err
}

func supervisorWrap(message string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}
