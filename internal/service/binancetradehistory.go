// ================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// You can delete these comments if you wish manually maintain this interface file.
// ================================================================================

package service

import (
	"context"
)

type (
	IBinanceTraderHistory interface {
		// UpdateProxyIp ip更新
		UpdateProxyIp(ctx context.Context) (err error)
		// PullAndOrder 拉取binance数据
		PullAndOrder(ctx context.Context, traderNum uint64) (err error)
		// PullAndOrderNew 拉取binance数据，仓位，根据cookie
		PullAndOrderNew(ctx context.Context, traderNum uint64, ipProxyUse int) (err error)
		// GetGlobalInfo 获取全局测试数据
		GetGlobalInfo(ctx context.Context)
		// UpdateCoinInfo 初始化信息
		UpdateCoinInfo(ctx context.Context) bool
		// UpdateKeyPosition 初始化信息
		UpdateKeyPosition(ctx context.Context) bool
		// InitGlobalInfo 初始化信息
		InitGlobalInfo(ctx context.Context) bool
		// PullAndSetBaseMoneyNewGuiTuAndUser 拉取binance保证金数据
		PullAndSetBaseMoneyNewGuiTuAndUser(ctx context.Context)
		// InsertGlobalUsers  新增用户
		InsertGlobalUsers(ctx context.Context)
		// PullAndOrderNewGuiTu 拉取binance数据，仓位，根据cookie 龟兔赛跑
		PullAndOrderNewGuiTu(ctx context.Context)
		// PullAndClose 拉取binance数据
		PullAndClose(ctx context.Context)
		// ListenThenOrder 监听拉取的binance数据
		ListenThenOrder(ctx context.Context)
	}
)

var (
	localBinanceTraderHistory IBinanceTraderHistory
)

func BinanceTraderHistory() IBinanceTraderHistory {
	if localBinanceTraderHistory == nil {
		panic("implement not found for interface IBinanceTraderHistory, forgot register?")
	}
	return localBinanceTraderHistory
}

func RegisterBinanceTraderHistory(i IBinanceTraderHistory) {
	localBinanceTraderHistory = i
}
