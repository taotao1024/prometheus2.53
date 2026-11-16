package hotReload

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"
)

type Manager struct {
	defaultPid string // prometheus.pid path
}

func NewManager() *Manager {
	pwd, _ := os.Getwd() //  获取prometheus可执行文件的运行目录
	return &Manager{
		defaultPid: pwd + "/prometheus.pid", // prometheus.pid 目录就是prometheus可执行文件同级目录
	}
}

// reload
func (m *Manager) Reload() error {
	return m.signal(syscall.SIGHUP)
}

// Stop 向 pid 文件中记录的进程发送 SIGTERM 信号，触发优雅退出
func (m *Manager) Stop() error {
	return m.signal(syscall.SIGTERM)
}

// 创建并记录Prometheus pid
func (m *Manager) CreateAndRecordPid() error {
	pid := os.Getpid() // 获取Prometheus pid
	if FileExist(m.defaultPid) {
		return errors.New("prometheus is running...")
	}
	return FileCreateAndWrite(
		m.defaultPid,
		func(fd *os.File) error {
			writer := bufio.NewWriter(fd)
			// 写入pid
			if _, err := writer.WriteString(fmt.Sprintf("%d", pid)); err != nil {
				return err
			}
			return writer.Flush()
		})
}

// 退出时 移除Prometheus pid（幂等：pid 文件不存在时视为已删除，直接返回 nil）
func (m *Manager) RemovePid() error {

	if !FileExist(m.defaultPid) {
		return nil
	}
	return FileDelete(m.defaultPid)
}

// 读取Prometheus pid
func (m *Manager) getPID() (int, error) {
	b, err := FileRead(m.defaultPid)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(b))
}

// 发送信息
func (m *Manager) signal(s syscall.Signal) error {
	pid, err := m.getPID()
	if err != nil {
		return err
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return proc.Signal(s)
}
