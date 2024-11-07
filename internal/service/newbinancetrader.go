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
	INewBinanceTrader interface {
		Test(ctx context.Context, num uint64) (err error)
		GetAllTraders(ctx context.Context) (traders []*entity.NewBinanceTrader, err error)
	}
)

var (
	localNewBinanceTrader INewBinanceTrader
)

func NewBinanceTrader() INewBinanceTrader {
	if localNewBinanceTrader == nil {
		panic("implement not found for interface INewBinanceTrader, forgot register?")
	}
	return localNewBinanceTrader
}

func RegisterNewBinanceTrader(i INewBinanceTrader) {
	localNewBinanceTrader = i
}
