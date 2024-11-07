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
	ITrader interface {
		Test(ctx context.Context, num uint64) (err error)
		GetAllTraders(ctx context.Context) (traders []*entity.Trader, err error)
	}
)

var (
	localTrader ITrader
)

func Trader() ITrader {
	if localTrader == nil {
		panic("implement not found for interface ITrader, forgot register?")
	}
	return localTrader
}

func RegisterTrader(i ITrader) {
	localTrader = i
}
