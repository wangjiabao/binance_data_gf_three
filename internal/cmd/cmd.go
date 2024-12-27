package cmd

import (
	"binance_data_gf/internal/service"
	"context"
	"fmt"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/net/ghttp"
	"github.com/gogf/gf/v2/os/gcmd"
	"github.com/gogf/gf/v2/os/gtimer"
	"strconv"
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

			// 初始化币种
			if !serviceBinanceTrader.UpdateCoinInfo(ctx) {
				fmt.Println("初始化币种失败，fail")
				return nil
			}
			fmt.Println("初始化币种成功，ok")

			// 拉取保证金，30秒/次，拉取保证金
			serviceBinanceTrader.PullAndSetBaseMoneyNewGuiTuAndUser(ctx)
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

			// 开启任务协程
			go serviceBinanceTrader.PullAndOrderBinanceByApi(ctx)

			// 开启http管理服务
			s := g.Server()
			s.Group("/api", func(group *ghttp.RouterGroup) {
				// 查询num
				group.GET("/nums", func(r *ghttp.Request) {
					res := serviceBinanceTrader.GetSystemUserNum(ctx)

					responseData := make([]*g.MapStrAny, 0)
					for k, v := range res {
						responseData = append(responseData, &g.MapStrAny{k: v})
					}

					r.Response.WriteJson(responseData)
					return
				})

				// 更新num
				group.POST("/update/num", func(r *ghttp.Request) {
					var (
						parseErr error
						setErr   error
						num      float64
					)
					num, parseErr = strconv.ParseFloat(r.PostFormValue("num"), 64)
					if nil != parseErr || 0 >= num {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					setErr = serviceBinanceTrader.SetSystemUserNum(ctx, r.PostFormValue("apiKey"), num)
					if nil != setErr {
						r.Response.WriteJson(g.Map{
							"code": -2,
						})

						return
					}

					r.Response.WriteJson(g.Map{
						"code": 1,
					})

					return
				})

				// 更新开新单
				group.POST("/update/useNewSystem", func(r *ghttp.Request) {
					var (
						parseErr error
						setErr   error
						status   uint64
					)
					status, parseErr = strconv.ParseUint(r.PostFormValue("status"), 10, 64)
					if nil != parseErr || 0 > status {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					setErr = serviceBinanceTrader.SetUseNewSystem(ctx, r.PostFormValue("apiKey"), status)
					if nil != setErr {
						r.Response.WriteJson(g.Map{
							"code": -2,
						})

						return
					}

					r.Response.WriteJson(g.Map{
						"code": 1,
					})

					return
				})

				// 查询用户系统仓位
				group.GET("/user/positions", func(r *ghttp.Request) {
					res := serviceBinanceTrader.GetSystemUserPositions(ctx, r.Get("apiKey").String())

					responseData := make([]*g.MapStrAny, 0)
					for k, v := range res {
						responseData = append(responseData, &g.MapStrAny{k: v})
					}

					r.Response.WriteJson(responseData)
					return
				})

				// 用户设置仓位
				group.POST("/user/update/position", func(r *ghttp.Request) {
					var (
						parseErr     error
						num          float64
						system       uint64
						allCloseGate uint64
					)
					num, parseErr = strconv.ParseFloat(r.PostFormValue("num"), 64)
					if nil != parseErr || 0 >= num {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					system, parseErr = strconv.ParseUint(r.PostFormValue("system"), 10, 64)
					if nil != parseErr || 0 > system {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					allCloseGate, parseErr = strconv.ParseUint(r.PostFormValue("allCloseGate"), 10, 64)
					if nil != parseErr || 0 > allCloseGate {
						r.Response.WriteJson(g.Map{
							"code": -1,
						})

						return
					}

					r.Response.WriteJson(g.Map{
						"code": serviceBinanceTrader.SetSystemUserPosition(
							ctx,
							system,
							allCloseGate,
							r.PostFormValue("apiKey"),
							r.PostFormValue("symbol"),
							r.PostFormValue("side"),
							r.PostFormValue("positionSide"),
							num,
						),
					})

					return
				})
			})

			s.SetPort(8100)
			s.Run()

			return nil
		},
	}
)
