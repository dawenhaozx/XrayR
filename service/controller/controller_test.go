package controller_test

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"testing"

	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf"

	"github.com/XrayR-project/XrayR/api"
	"github.com/XrayR-project/XrayR/api/newV2board"
	_ "github.com/XrayR-project/XrayR/cmd/distro/all"
	"github.com/XrayR-project/XrayR/common/mylego"
	. "github.com/XrayR-project/XrayR/service/controller"
)

func TestController(t *testing.T) {
	serverConfig := &conf.Config{
		Stats:     &conf.StatsConfig{},
		LogConfig: &conf.LogConfig{LogLevel: "debug"},
	}
	policyConfig := &conf.PolicyConfig{}
	policyConfig.Levels = map[uint32]*conf.Policy{0: {
		StatsUserUplink:   true,
		StatsUserDownlink: true,
	}}
	serverConfig.Policy = policyConfig
	config, _ := serverConfig.Build()

	// config := &core.Config{
	// 	App: []*serial.TypedMessage{
	// 		serial.ToTypedMessage(&dispatcher.Config{}),
	// 		serial.ToTypedMessage(&proxyman.InboundConfig{}),
	// 		serial.ToTypedMessage(&proxyman.OutboundConfig{}),
	// 		serial.ToTypedMessage(&stats.Config{}),
	// 	}}

	server, err := core.New(config)
	defer server.Close()
	if err != nil {
		t.Errorf("failed to create instance: %s", err)
	}
	if err = server.Start(); err != nil {
		t.Errorf("Failed to start instance: %s", err)
	}
	certConfig := &mylego.CertConfig{
		CertMode:   "http",
		CertDomain: "test.ss.tk",
		Provider:   "alidns",
		Email:      "ss@ss.com",
	}
	controlerConfig := &Config{
		UpdatePeriodic: 5,
		CertConfig:     certConfig,
	}
	apiConfig := &api.Config{
		APIHost:  "http://127.0.0.1:667",
		Key:      "123",
		NodeID:   41,
		NodeType: "V2ray",
	}
	apiClient := newV2board.New(apiConfig)
	c := New(server, apiClient, controlerConfig, "newV2board")
	fmt.Println("Sleep 1s")
	err = c.Start()
	if err != nil {
		t.Error(err)
	}
	// Explicitly triggering GC to remove garbage from config loading.
	runtime.GC()

	{
		osSignals := make(chan os.Signal, 1)
		signal.Notify(osSignals, os.Interrupt, os.Kill, syscall.SIGTERM)
		<-osSignals
	}
}
