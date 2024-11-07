package logic

import (
	"binance_data_gf/internal/model/entity"
	"context"
	"fmt"
	"github.com/gogf/gf/v2/frame/g"
	"time"

	"binance_data_gf/internal/service"
)

type (
	sNewBinanceTrader struct{}
)

func init() {
	service.RegisterNewBinanceTrader(New())
}

func New() *sNewBinanceTrader {
	return &sNewBinanceTrader{}
}

func (s *sNewBinanceTrader) Test(ctx context.Context, num uint64) (err error) {
	fmt.Println(num, "秒的协程")
	time.Sleep(time.Second * time.Duration(num))
	return nil
}

func (s *sNewBinanceTrader) GetAllTraders(ctx context.Context) (traders []*entity.NewBinanceTrader, err error) {
	err = g.Model("new_binance_trader").Ctx(ctx).Where("status=?", 0).Scan(&traders)
	return traders, err
}
