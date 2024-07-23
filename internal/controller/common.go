package controller

import (
	"context"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v4/process"
	"golang.corp.yxkj.com/orange/cadb/internal/grpc/proto/proto"
)

type CommonHelper struct{}

// Ping
// 网络检测
func (helper *CommonHelper) Ping(context.Context, *proto.Empty) (*proto.PingResponse, error) {
	return &proto.PingResponse{
		Message:   "pong",
		Timestamp: time.Now().Unix(),
	}, nil
}

// Version
// 版本信息
func (helper *CommonHelper) Version(context.Context, *proto.Empty) (*proto.VersionResponse, error) {
	goVersion := runtime.Version()

	return &proto.VersionResponse{
		Version:   "v1.0.0",
		AppName:   "cadb",
		GitHash:   "",
		GitBranch: "",
		BuildTime: "",
		Goos:      runtime.GOOS,
		GoVersion: goVersion,
	}, nil
}
func (helper *CommonHelper) Heartbeat(context.Context, *proto.Empty) (*proto.HeartbeatResponse, error) {
	pid := os.Getpid()

	list, err := process.Processes()
	if err != nil {
		return nil, err
	}

	cpuUse := float32(0)
	memUse := float32(0)
	for _, p := range list {
		if p.Pid == int32(pid) {
			cpu, err := p.CPUPercent()
			if err == nil {
				cpuUse = float32(cpu)
			}

			mem, err := p.MemoryInfo()
			if err == nil {
				memUse = float32(mem.RSS)
			}

			break
		}
	}

	return &proto.HeartbeatResponse{
		Message:     "pong",
		Timestamp:   time.Now().Unix(),
		MemoryUsage: memUse,
		CpuUsage:    cpuUse,
	}, nil
}
