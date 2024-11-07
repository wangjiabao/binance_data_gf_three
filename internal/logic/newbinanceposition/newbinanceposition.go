package logic

import (
	"binance_data_gf/internal/model/entity"
	"binance_data_gf/internal/service"
	"context"
	"github.com/gogf/gf/v2/frame/g"
	"strconv"
)

type (
	sNewBinancePosition struct{}
)

func init() {
	service.RegisterNewBinancePosition(New())
}

func New() *sNewBinancePosition {
	return &sNewBinancePosition{}
}

func (s *sNewBinancePosition) GetByTraderNumNotClosed(ctx context.Context, traderNum uint64) (binancePositionNewestGroup []*entity.NewBinancePosition, err error) {
	err = g.Model("new_binance_" + strconv.FormatUint(traderNum, 10) + "_position").Ctx(ctx).OrderDesc("id").Scan(&binancePositionNewestGroup)
	if nil != err {
		return binancePositionNewestGroup, err
	}
	return binancePositionNewestGroup, err
}
