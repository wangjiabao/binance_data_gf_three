package logic

import (
	"binance_data_gf/internal/model/entity"
	"binance_data_gf/internal/service"
	"context"
	"github.com/gogf/gf/v2/frame/g"
	"strconv"
)

type (
	sNewBinancePositionHistory struct{}
)

func init() {
	service.RegisterNewBinancePositionHistory(New())
}

func New() *sNewBinancePositionHistory {
	return &sNewBinancePositionHistory{}
}

func (s *sNewBinancePositionHistory) GetByTraderNumNotClosed(ctx context.Context, traderNum uint64) (binancePositionHistoryNewestGroup []*entity.NewBinancePositionHistory, err error) {
	err = g.Model("new_binance_position_" + strconv.FormatUint(traderNum, 10) + "_history").Ctx(ctx).OrderDesc("id").Scan(&binancePositionHistoryNewestGroup)
	if nil != err {
		return binancePositionHistoryNewestGroup, err
	}
	return binancePositionHistoryNewestGroup, err
}
