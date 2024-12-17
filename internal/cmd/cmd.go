package cmd

import (
	"binance_data_gf/internal/service"
	"context"
	"fmt"
	"github.com/gogf/gf/v2/os/gcmd"
	"github.com/gogf/gf/v2/os/gtimer"
	"time"
)

var (
	Main = &gcmd.Command{
		Name: "main",
	}

	// Trader 监听系统中指定的交易员-龟兔赛跑
	Trader = &gcmd.Command{
		Name:  "trader",
		Brief: "listen trader",
		Func: func(ctx context.Context, parser *gcmd.Parser) (err error) {
			serviceBinanceTrader := service.BinanceTraderHistory()

			// 初始化根据数据库现有人
			if !serviceBinanceTrader.UpdateCoinInfo(ctx) {
				fmt.Println("初始化币种失败，fail")
				return nil
			}
			fmt.Println("初始化币种成功，ok")

			// 拉龟兔的保证金
			serviceBinanceTrader.PullAndSetBaseMoneyNewGuiTuAndUser(ctx)

			// 10秒/次，拉取保证金
			handle := func(ctx context.Context) {
				serviceBinanceTrader.PullAndSetBaseMoneyNewGuiTuAndUser(ctx)
			}
			gtimer.AddSingleton(ctx, time.Second*30, handle)

			// 30秒/次，加新人
			handle2 := func(ctx context.Context) {
				serviceBinanceTrader.InsertGlobalUsers(ctx)
			}
			gtimer.AddSingleton(ctx, time.Second*30, handle2)

			// 300秒/次，币种信息
			handle3 := func(ctx context.Context) {
				serviceBinanceTrader.UpdateCoinInfo(ctx)
			}
			gtimer.AddSingleton(ctx, time.Second*300, handle3)

			// 100秒/次，仓位信息落库
			handle4 := func(ctx context.Context) {
				serviceBinanceTrader.UpdateKeyPosition(ctx)
			}
			gtimer.AddSingleton(ctx, time.Second*100, handle4)

			//// 15秒/次，测试
			//handle5 := func(ctx context.Context) {
			//	serviceBinanceTrader.GetGlobalInfo(ctx)
			//}
			//gtimer.AddSingleton(ctx, time.Second*60, handle5)

			serviceBinanceTrader.PullAndOrderBinanceByApi(ctx)
			return nil

			//// 任务1 同步订单，死循环
			//serviceBinanceTrader.PullAndOrderNewGuiTu(ctx)
			//
			//// 阻塞
			//select {}
		},
	}
)
