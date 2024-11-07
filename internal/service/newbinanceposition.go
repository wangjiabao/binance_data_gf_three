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
	INewBinancePosition interface {
		GetByTraderNumNotClosed(ctx context.Context, traderNum uint64) (binancePositionNewestGroup []*entity.NewBinancePosition, err error)
	}
)

var (
	localNewBinancePosition INewBinancePosition
)

func NewBinancePosition() INewBinancePosition {
	if localNewBinancePosition == nil {
		panic("implement not found for interface INewBinancePosition, forgot register?")
	}
	return localNewBinancePosition
}

func RegisterNewBinancePosition(i INewBinancePosition) {
	localNewBinancePosition = i
}
