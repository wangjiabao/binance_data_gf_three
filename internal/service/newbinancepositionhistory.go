// ================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// You can delete these comments if you wish manually maintain this interface file.
// ================================================================================

package service

import (
	"binance_data_gf/internal/model/entity"
	"context"
)

type (
	INewBinancePositionHistory interface {
		GetByTraderNumNotClosed(ctx context.Context, traderNum uint64) (binancePositionHistoryNewestGroup []*entity.NewBinancePositionHistory, err error)
	}
)

var (
	localNewBinancePositionHistory INewBinancePositionHistory
)

func NewBinancePositionHistory() INewBinancePositionHistory {
	if localNewBinancePositionHistory == nil {
		panic("implement not found for interface INewBinancePositionHistory, forgot register?")
	}
	return localNewBinancePositionHistory
}

func RegisterNewBinancePositionHistory(i INewBinancePositionHistory) {
	localNewBinancePositionHistory = i
}
