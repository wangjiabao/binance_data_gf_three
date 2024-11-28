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
		// GetGlobalInfo 获取全局测试数据
		GetGlobalInfo(ctx context.Context)
		// UpdateCoinInfo 初始化信息
		UpdateCoinInfo(ctx context.Context) bool
		// PullPlatCoinInfo 获取平台的币种信息
		PullPlatCoinInfo(ctx context.Context) bool
		// UpdateKeyPosition 更新keyPosition信息
		UpdateKeyPosition(ctx context.Context) bool
		// InitGlobalInfo 初始化信息
		InitGlobalInfo(ctx context.Context) bool
		// PullAndSetBaseMoneyNewGuiTuAndUser 拉取binance保证金数据
		PullAndSetBaseMoneyNewGuiTuAndUser(ctx context.Context)
		// InsertGlobalUsers  新增用户
		InsertGlobalUsers(ctx context.Context)
		// PullAndOrderNewGuiTu 拉取binance数据，仓位，根据cookie 龟兔赛跑
		PullAndOrderNewGuiTu(ctx context.Context)
		// PullAndOrderBinanceByApi 拉取binance数据，仓位，根据cookie 龟兔赛跑
		PullAndOrderBinanceByApi(ctx context.Context)
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
