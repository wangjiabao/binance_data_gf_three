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
	sTrader struct{}
)

func init() {
	service.RegisterTrader(New())
}

func New() *sTrader {
	return &sTrader{}
}

func (s *sTrader) Test(ctx context.Context, num uint64) (err error) {
	fmt.Println(num, "秒的协程")
	time.Sleep(time.Second * time.Duration(num))
	return nil
}

func (s *sTrader) GetAllTraders(ctx context.Context) (traders []*entity.Trader, err error) {
	err = g.Model("trader").Ctx(ctx).Where("is_open=?", 1).Scan(&traders)
	return traders, err
}
