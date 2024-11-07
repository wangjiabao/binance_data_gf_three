package logic

import (
	"binance_data_gf/internal/model/do"
	"binance_data_gf/internal/model/entity"
	"binance_data_gf/internal/service"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/container/gqueue"
	"github.com/gogf/gf/v2/container/gset"
	"github.com/gogf/gf/v2/container/gtype"
	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/grpool"
	"github.com/gogf/gf/v2/os/gtime"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	sBinanceTraderHistory struct {
		// 全局存储
		pool *grpool.Pool
		// 针对binance拉取交易员下单接口定制的数据结构，每页数据即使同一个ip不同的交易员是可以返回数据，在逻辑上和这个设计方式要达成共识再设计后续的程序逻辑。
		// 考虑实际的业务场景，接口最多6000条，每次查50条，最多需要120个槽位。假设，每10个槽位一循环，意味着对应同一个交易员每次并行使用10个ip查询10页数据。
		//ips map[int]*proxyData
		ips        *gmap.IntStrMap
		orderQueue *gqueue.Queue
	}
)

func init() {
	service.RegisterBinanceTraderHistory(New())
}

func New() *sBinanceTraderHistory {
	return &sBinanceTraderHistory{
		grpool.New(), // 这里是请求协程池子，可以配合着可并行请求binance的限制使用，来限制最大共存数，后续jobs都将排队，考虑到上层的定时任务
		gmap.NewIntStrMap(true),
		gqueue.New(), // 下单顺序队列
	}
}

func IsEqual(f1, f2 float64) bool {
	if f1 > f2 {
		return f1-f2 < 0.000000001
	} else {
		return f2-f1 < 0.000000001
	}
}

func lessThanOrEqualZero(a, b float64, epsilon float64) bool {
	return a-b < epsilon || math.Abs(a-b) < epsilon
}

type proxyData struct {
	Ip   string
	Port int64
}

type proxyRep struct {
	Data []*proxyData
}

// 拉取代理列表，暂时弃用
func requestProxy() ([]*proxyData, error) {
	var (
		resp   *http.Response
		b      []byte
		res    []*proxyData
		err    error
		apiUrl = ""
	)

	httpClient := &http.Client{
		Timeout: time.Second * 10,
	}

	// 构造请求
	resp, err = httpClient.Get(apiUrl)
	if err != nil {
		return nil, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var l *proxyRep
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	res = make([]*proxyData, 0)
	if nil == l.Data || 0 >= len(l.Data) {
		return res, nil
	}

	for _, v := range l.Data {
		res = append(res, v)
	}

	return res, nil
}

// UpdateProxyIp ip更新
func (s *sBinanceTraderHistory) UpdateProxyIp(ctx context.Context) (err error) {
	// 20个客户端代理，这里注意一定是key是从0开始到size-1的
	s.ips.Set(0, "http://43.133.175.121:888/")
	s.ips.Set(1, "http://43.133.209.89:888/")
	s.ips.Set(2, "http://43.133.177.73:888/")
	s.ips.Set(3, "http://43.163.224.155:888/")
	s.ips.Set(4, "http://43.163.210.110:888/")
	s.ips.Set(5, "http://43.163.242.180:888/")
	s.ips.Set(6, "http://43.153.162.213:888/")
	s.ips.Set(7, "http://43.163.197.191:888/")
	s.ips.Set(8, "http://43.163.217.70:888/")
	s.ips.Set(9, "http://43.163.194.135:888/")
	s.ips.Set(10, "http://43.130.254.24:888/")
	s.ips.Set(11, "http://43.128.249.92:888/")
	s.ips.Set(12, "http://43.153.177.107:888/")
	s.ips.Set(13, "http://43.133.15.135:888/")
	s.ips.Set(14, "http://43.130.229.66:888/")
	s.ips.Set(15, "http://43.133.196.220:888/")
	s.ips.Set(16, "http://43.163.208.36:888/")
	s.ips.Set(17, "http://43.133.204.254:888/")
	s.ips.Set(18, "http://43.153.181.89:888/")
	s.ips.Set(19, "http://43.163.231.217:888/")

	return nil

	//res := make([]*proxyData, 0)
	//for i := 0; i < 1000; i++ {
	//	var (
	//		resTmp []*proxyData
	//	)
	//	resTmp, err = requestProxy()
	//	if nil != err {
	//		fmt.Println("ip池子更新出错", err)
	//		time.Sleep(time.Second * 1)
	//		continue
	//	}
	//
	//	if 0 < len(resTmp) {
	//		res = append(res, resTmp...)
	//	}
	//
	//	if 20 <= len(res) { // 20个最小
	//		break
	//	}
	//
	//	fmt.Println("ip池子更新时无数据")
	//	time.Sleep(time.Second * 1)
	//}
	//
	//s.ips.Clear()
	//
	//// 更新
	//for k, v := range res {
	//	s.ips.Set(k, "http://"+v.Ip+":"+strconv.FormatInt(v.Port, 10)+"/")
	//}
	//
	//fmt.Println("ip池子更新成功", time.Now(), s.ips.Size())
	//return nil
}

// PullAndOrder 拉取binance数据
func (s *sBinanceTraderHistory) PullAndOrder(ctx context.Context, traderNum uint64) (err error) {
	start := time.Now()

	//// 测试部分注释
	//_, err = s.pullAndSetHandle(ctx, traderNum, 120) // 执行
	//fmt.Println("ok", traderNum)
	//return nil

	// 测试部分注释
	//if 1 == traderNum {
	//	fmt.Println("此时系统，workers：", pool.Size(), "jobs：", pool.Jobs())
	//	return nil
	//}

	/**
	 * 这里说明一下同步规则
	 * 首先任务已经是每个交易员的单独协程了
	 *
	 * 步骤1：
	 * 探测任务：拉取第1页10条，前10条与数据库中前10条对比，一摸一样认为无新订单（继续进行探测），否则有新订单进入步骤2。
	 * 步骤2：
	 * 下单任务：并行5个任务，每个任务表示每页数据的拉取，每次5页共250条，拉取后重新拉取第1页数据与刚才的5页中的第1页，
	 * 对比10条，一模一样表示这段任务执行期间无更新，否则全部放弃，重新开始步骤2。
	 *
	 * ip池子的加入
	 * 步骤1中探测任务可以考虑分配一个ip。
	 * 步骤2中每个任务分配不同的ip（防止ip封禁用，目前经验是binance对每个ip在查询每个交易员数据时有2秒的限制，并行则需要不同的ip）
	 *
	 * 数据库不存在数据时，直接执行步骤2，并行5页，如果执行完任务，发现有新订单，则全部放弃，重新步骤2。
	 */

	// 数据库对比数据
	var (
		compareMax                     = 10 // 预设最大对比条数，小于最大限制10条，注意：不能超过50条，在程序多出有写死，binance目前每页最大条数
		currentCompareMax              int  // 实际获得对比条数
		binanceTradeHistoryNewestGroup []*entity.NewBinanceTradeHistory
		resData                        []*entity.NewBinanceTradeHistory
		resDataCompare                 []*entity.NewBinanceTradeHistory
		initPull                       bool
	)

	err = g.Model("new_binance_trade_" + strconv.FormatUint(traderNum, 10) + "_history").Ctx(ctx).Limit(compareMax).OrderDesc("id").Scan(&binanceTradeHistoryNewestGroup)
	if nil != err {
		return err
	}

	currentCompareMax = len(binanceTradeHistoryNewestGroup)

	ipMapNeedWait := make(map[string]bool, 0) // 刚使用的ip，大概率加快查询速度，2s以内别用的ip
	// 数据库无数据，拉取满额6000条数据
	if 0 >= currentCompareMax {
		initPull = true
		resData, err = s.pullAndSetHandle(ctx, traderNum, 120, true, ipMapNeedWait) // 执行
		if nil != err {
			fmt.Println("初始化，执行拉取数据协程异常，错误信息：", err, "交易员：", traderNum)
		}

		if nil == resData {
			fmt.Println("初始化，执行拉取数据协程异常，数据缺失", "交易员：", traderNum)
			return nil
		}

		if 0 >= len(resData) {
			fmt.Println("初始化，执行拉取数据协程异常，空数据：", len(resData), "交易员：", traderNum)
			return nil
		}

		var (
			compareResDiff bool
		)
		// 截取前10条记录
		afterCompare := make([]*entity.NewBinanceTradeHistory, 0)
		if len(resData) >= compareMax {
			afterCompare = resData[:compareMax]
		} else {
			// 这里也限制了最小入库的交易员数据条数
			fmt.Println("初始化，执行拉取数据协程异常，条数不足，条数：", len(resData), "交易员：", traderNum)
			return nil
		}

		if 0 >= len(afterCompare) {
			fmt.Println("初始化，执行拉取数据协程异常，条数不足，条数：", len(resData), "交易员：", traderNum)
			return nil
		}

		// todo 如果未来初始化仓位不准，那这里应该很多的嫌疑，因为在拉取中第一页数据的协程最后执行完，那么即使带单员更新了，也不会被察觉，当然概率很小
		// 有问题的话，可以换成重新执行一次完整的拉取，比较
		_, _, compareResDiff, err = s.compareBinanceTradeHistoryPageOne(int64(compareMax), traderNum, afterCompare)
		if nil != err {
			return err
		}

		// 不同，则拉取数据的时间有新单，放弃操作，等待下次执行
		if compareResDiff {
			fmt.Println("初始化，执行拉取数据协程异常，有新单", "交易员：", traderNum)
			return nil
		}

	} else if compareMax <= currentCompareMax {
		/**
		 * 试探开始
		 * todo
		 * 假设：binance的数据同一时间的数据乱序出现时，因为和数据库前n条不一致，而认为是新数据，后续保存会有处理，但是这里就会一直生效，现在这个假设还未出现。*
		 * 因为保存对上述假设的限制，延迟出现的同一时刻的数据一直不会被系统保存，而每次都会触发这里的比较，得到不同数据，为了防止乱序的假设最后是这样做，但是可能导致一直拉取10页流量增长，后续观察假设不存在最好，假设存在更新方案。
		 */
		var (
			newData        []*binanceTradeHistoryDataList
			compareResDiff bool
		)

		ipMapNeedWait, newData, compareResDiff, err = s.compareBinanceTradeHistoryPageOne(int64(compareMax), traderNum, binanceTradeHistoryNewestGroup)
		if nil != err {
			return err
		}

		// 相同，返回
		if !compareResDiff {
			return nil
		}

		if nil == newData || 0 >= len(newData) {
			fmt.Println("日常，执行拉取数据协程异常，新数据未空，错误信息：", err, "交易员：", traderNum)
			return nil
		}

		fmt.Println("新数据：交易员：", traderNum)

		// 不同，开始捕获
		resData, err = s.pullAndSetHandle(ctx, traderNum, 10, true, ipMapNeedWait) // todo 执行，目前猜测最大500条，根据经验拍脑袋
		if nil != err {
			fmt.Println("日常，执行拉取数据协程异常，错误信息：", err, "交易员：", traderNum)
		}

		if nil == resData {
			fmt.Println("日常，执行拉取数据协程异常，数据缺失", "交易员：", traderNum)
			return nil
		}

		if 0 >= len(resData) {
			fmt.Println("日常，执行拉取数据协程异常，数据为空", "交易员：", traderNum)
			return nil
		}

		// 重新拉取，比较探测的结果，和最后的锁定结果
		resDataCompare, err = s.pullAndSetHandle(ctx, traderNum, 1, false, ipMapNeedWait) // todo 执行，目前猜测最大500条，根据经验拍脑袋
		if nil != err {
			fmt.Println("日常，执行拉取数据协程异常，比较数据，错误信息：", err, "交易员：", traderNum)
		}

		if nil == resDataCompare {
			fmt.Println("日常，执行拉取数据协程异常，比较数据，数据缺失", "交易员：", traderNum)
			return nil
		}

		for kNewData, vNewData := range newData {
			// 比较
			if !(vNewData.Time == resDataCompare[kNewData].Time &&
				vNewData.Symbol == resDataCompare[kNewData].Symbol &&
				vNewData.Side == resDataCompare[kNewData].Side &&
				vNewData.PositionSide == resDataCompare[kNewData].PositionSide &&
				IsEqual(vNewData.Qty, resDataCompare[kNewData].Qty) && // 数量
				IsEqual(vNewData.Price, resDataCompare[kNewData].Price) && //价格
				IsEqual(vNewData.RealizedProfit, resDataCompare[kNewData].RealizedProfit) &&
				IsEqual(vNewData.Quantity, resDataCompare[kNewData].Quantity) &&
				IsEqual(vNewData.Fee, resDataCompare[kNewData].Fee)) {
				fmt.Println("日常，执行拉取数据协程异常，比较数据，数据不同，第一条", "交易员：", traderNum, resData[0], resDataCompare[0])
				return nil
			}
		}

	} else {
		fmt.Println("执行拉取数据协程异常，查询数据库数据条数不在范围：", compareMax, currentCompareMax, "交易员：", traderNum, "初始化：", initPull)
	}

	// 时长
	fmt.Printf("程序拉取部分，开始 %v, 时长: %v, 交易员: %v\n", start, time.Since(start), traderNum)

	// 非初始化，截断数据
	if !initPull {
		tmpResData := make([]*entity.NewBinanceTradeHistory, 0)
		//tmpCurrentCompareMax := currentCompareMax
		for _, vResData := range resData {
			/**
			 * todo
			 * 停下的条件，暂时是：
			 * 1 数据库的最新一条比较，遇到时间，币种，方向一致的订单，认为是已经纳入数据库的。
			 * 2 如果数据库最新的时间已经晚于遍历时遇到的时间也一定停下，这里会有一个好处，即使binance的数据同一时间的数据总是乱序出现时，我们也不会因为和数据库第一条不一致，而认为是新数据。
			 *
			 * 如果存在误判的原因，
			 * 1 情况是在上次执行完拉取保存后，相同时间的数据，因为binance的问题，延迟出现了。
			 */
			if vResData.Time == binanceTradeHistoryNewestGroup[0].Time &&
				vResData.Side == binanceTradeHistoryNewestGroup[0].Side &&
				vResData.PositionSide == binanceTradeHistoryNewestGroup[0].PositionSide &&
				vResData.Symbol == binanceTradeHistoryNewestGroup[0].Symbol {
				break
			}

			if vResData.Time <= binanceTradeHistoryNewestGroup[0].Time {
				fmt.Println("遍历时竟然未发现相同数据！此时数据时间已经小于了数据库最新一条的时间，如果时间相同可能是binance延迟出现的数据，数据：", vResData, binanceTradeHistoryNewestGroup[0])
				break
			}

			//if (len(resData) - k) <= tmpCurrentCompareMax { // 还剩下几条
			//	tmpCurrentCompareMax = len(resData) - k
			//}

			//tmp := 0
			//if 0 < tmpCurrentCompareMax {
			//	for i := 0; i < tmpCurrentCompareMax; i++ { // todo 如果只剩下最大条数以内的数字，只能兼容着比较，这里根据经验判断会不会出现吧
			//		if resData[k+i].Time == binanceTradeHistoryNewestGroup[i].Time &&
			//			resData[k+i].Symbol == binanceTradeHistoryNewestGroup[i].Symbol &&
			//			resData[k+i].Side == binanceTradeHistoryNewestGroup[i].Side &&
			//			resData[k+i].PositionSide == binanceTradeHistoryNewestGroup[i].PositionSide &&
			//			IsEqual(resData[k+i].Qty, binanceTradeHistoryNewestGroup[i].Qty) && // 数量
			//			IsEqual(resData[k+i].Price, binanceTradeHistoryNewestGroup[i].Price) && //价格
			//			IsEqual(resData[k+i].RealizedProfit, binanceTradeHistoryNewestGroup[i].RealizedProfit) &&
			//			IsEqual(resData[k+i].Quantity, binanceTradeHistoryNewestGroup[i].Quantity) &&
			//			IsEqual(resData[k+i].Fee, binanceTradeHistoryNewestGroup[i].Fee) {
			//			tmp++
			//		}
			//	}
			//
			//	if tmpCurrentCompareMax == tmp {
			//		break
			//	}
			//} else {
			//	break
			//}

			tmpResData = append(tmpResData, vResData)
		}

		resData = tmpResData
	}

	insertData := make([]*do.NewBinanceTradeHistory, 0)
	// 数据倒序插入，程序走到这里，最多会拉下来初始化：6000，日常：500，最少10条（前边的条件限制）
	for i := len(resData) - 1; i >= 0; i-- {
		insertData = append(insertData, &do.NewBinanceTradeHistory{
			Time:                resData[i].Time,
			Symbol:              resData[i].Symbol,
			Side:                resData[i].Side,
			PositionSide:        resData[i].PositionSide,
			Price:               resData[i].Price,
			Fee:                 resData[i].Fee,
			FeeAsset:            resData[i].FeeAsset,
			Quantity:            resData[i].Quantity,
			QuantityAsset:       resData[i].QuantityAsset,
			RealizedProfit:      resData[i].RealizedProfit,
			RealizedProfitAsset: resData[i].RealizedProfitAsset,
			BaseAsset:           resData[i].BaseAsset,
			Qty:                 resData[i].Qty,
			ActiveBuy:           resData[i].ActiveBuy,
		})
	}

	// 入库
	if 0 >= len(insertData) {
		return nil
	}

	// 推入下单队列
	// todo 这种队列的方式可能存在生产者或消费者出现问题，而丢单的情况，可以考虑更换复杂的方式，即使丢单开不起来会影响开单，关单的话少关单，有仓位检测的二重保障
	pushDataMap := make(map[string]*binanceTrade, 0)
	pushData := make([]*binanceTrade, 0)

	// 代币
	for _, vInsertData := range insertData {
		// 代币，仓位，方向，同一秒 暂时看作一次下单
		timeTmp := vInsertData.Time.(uint64)
		if _, ok := pushDataMap[vInsertData.Symbol.(string)+vInsertData.PositionSide.(string)+vInsertData.Side.(string)+strconv.FormatUint(timeTmp, 10)]; !ok {
			pushDataMap[vInsertData.Symbol.(string)+vInsertData.PositionSide.(string)+vInsertData.Side.(string)+strconv.FormatUint(timeTmp, 10)] = &binanceTrade{
				TraderNum: traderNum,
				Type:      vInsertData.PositionSide.(string),
				Symbol:    vInsertData.Symbol.(string),
				Side:      vInsertData.Side.(string),
				Position:  "",
				Qty:       "",
				QtyFloat:  vInsertData.Qty.(float64),
				Time:      timeTmp,
			}
		} else { // 到这里一定存在了，累加
			pushDataMap[vInsertData.Symbol.(string)+vInsertData.PositionSide.(string)+vInsertData.Side.(string)+strconv.FormatUint(timeTmp, 10)].QtyFloat += vInsertData.Qty.(float64)
		}
	}

	if 0 < len(pushDataMap) {
		for _, vPushDataMap := range pushDataMap {
			pushData = append(pushData, vPushDataMap)
		}

		if 0 < len(pushData) {
			// 排序，时间靠前的在前边处理
			sort.Slice(pushData, func(i, j int) bool {
				return pushData[i].Time < pushData[j].Time
			})
		}
	}

	err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
		// 日常更新数据
		if 0 < len(pushData) {
			// 先查更新仓位，代币，仓位，方向归集好
			for _, vPushDataMap := range pushData {
				// 查询最新未关仓仓位
				var (
					selectOne []*entity.NewBinancePositionHistory
				)
				err = tx.Ctx(ctx).Model("new_binance_position_"+strconv.FormatUint(traderNum, 10)+"_history").
					Where("symbol=?", vPushDataMap.Symbol).Where("side=?", vPushDataMap.Type).Where("opened<=?", vPushDataMap.Time).Where("closed=?", 0).Where("qty>?", 0).
					OrderDesc("id").Limit(1).Scan(&selectOne)
				if err != nil {
					return err
				}

				if 0 >= len(selectOne) {
					// 新增仓位
					if ("LONG" == vPushDataMap.Type && "BUY" == vPushDataMap.Side) ||
						("SHORT" == vPushDataMap.Type && "SELL" == vPushDataMap.Side) {
						// 开仓
						_, err = tx.Ctx(ctx).Insert("new_binance_position_"+strconv.FormatUint(traderNum, 10)+"_history", &do.NewBinancePositionHistory{
							Closed: 0,
							Opened: vPushDataMap.Time,
							Symbol: vPushDataMap.Symbol,
							Side:   vPushDataMap.Type,
							Status: "",
							Qty:    vPushDataMap.QtyFloat,
						})
						if err != nil {
							return err
						}
					}
				} else {
					// 修改仓位
					if ("LONG" == vPushDataMap.Type && "SELL" == vPushDataMap.Side) ||
						("SHORT" == vPushDataMap.Type && "BUY" == vPushDataMap.Side) {
						// 平空 || 平多
						var (
							updateData g.Map
						)

						// todo bsc最高精度小数点7位，到了6位的情况非常少，没意义，几乎等于完全平仓
						if lessThanOrEqualZero(selectOne[0].Qty, vPushDataMap.QtyFloat, 1e-6) {
							updateData = g.Map{
								"qty":    0,
								"closed": gtime.Now().UnixMilli(),
							}

							// 平仓前仓位
							vPushDataMap.Position = strconv.FormatFloat(vPushDataMap.QtyFloat, 'f', -1, 64)
						} else {
							updateData = g.Map{
								"qty": &gdb.Counter{
									Field: "qty",
									Value: -vPushDataMap.QtyFloat, // 加 -值
								},
							}

							// 平仓前仓位
							vPushDataMap.Position = strconv.FormatFloat(selectOne[0].Qty, 'f', -1, 64)
						}

						_, err = tx.Ctx(ctx).Update("new_binance_position_"+strconv.FormatUint(traderNum, 10)+"_history", updateData, "id", selectOne[0].Id)
						if nil != err {
							return err
						}

					} else if ("LONG" == vPushDataMap.Type && "BUY" == vPushDataMap.Side) ||
						("SHORT" == vPushDataMap.Type && "SELL" == vPushDataMap.Side) {
						// 开多 || 开空
						updateData := g.Map{
							"qty": &gdb.Counter{
								Field: "qty",
								Value: vPushDataMap.QtyFloat,
							},
						}

						_, err = tx.Ctx(ctx).Update("new_binance_position_"+strconv.FormatUint(traderNum, 10)+"_history", updateData, "id", selectOne[0].Id)
						if nil != err {
							return err
						}
					}
				}
			}
		}

		batchSize := 500
		for i := 0; i < len(insertData); i += batchSize {
			end := i + batchSize
			if end > len(insertData) {
				end = len(insertData)
			}
			batch := insertData[i:end]

			_, err = tx.Ctx(ctx).Insert("new_binance_trade_"+strconv.FormatUint(traderNum, 10)+"_history", batch)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if nil != err {
		return err
	}

	// 推入下单队列
	// todo 这种队列的方式可能存在生产者或消费者出现问题，而丢单的情况，可以考虑更换复杂的方式，即使丢单开不起来会影响开单，关单的话少关单，有仓位检测的二重保障
	if !initPull && 0 < len(pushData) {
		s.orderQueue.Push(pushData)
	}

	fmt.Println("更新结束，交易员：", traderNum)
	return nil
}

// PullAndOrderNew 拉取binance数据，仓位，根据cookie
func (s *sBinanceTraderHistory) PullAndOrderNew(ctx context.Context, traderNum uint64, ipProxyUse int) (err error) {
	//start := time.Now()
	//var (
	//	trader                    []*entity.Trader
	//	zyTraderCookie            []*entity.ZyTraderCookie
	//	binancePosition           []*entity.TraderPosition
	//	binancePositionMap        map[string]*entity.TraderPosition
	//	binancePositionMapCompare map[string]*entity.TraderPosition
	//	reqResData                []*binancePositionDataList
	//)
	//
	//// 数据库必须信息
	//err = g.Model("trader_position_" + strconv.FormatUint(traderNum, 10)).Ctx(ctx).Scan(&binancePosition)
	//if nil != err {
	//	return err
	//}
	//binancePositionMap = make(map[string]*entity.TraderPosition, 0)
	//binancePositionMapCompare = make(map[string]*entity.TraderPosition, 0)
	//for _, vBinancePosition := range binancePosition {
	//	binancePositionMap[vBinancePosition.Symbol+vBinancePosition.PositionSide] = vBinancePosition
	//	binancePositionMapCompare[vBinancePosition.Symbol+vBinancePosition.PositionSide] = vBinancePosition
	//}
	//
	//// 数据库必须信息
	//err = g.Model("trader").Ctx(ctx).Where("portfolioId=?", traderNum).OrderDesc("id").Limit(1).Scan(&trader)
	//if nil != err {
	//	return err
	//}
	//if 0 >= len(trader) {
	//	//fmt.Println("新，不存在trader表：信息", traderNum, ipProxyUse)
	//	return nil
	//}
	//
	//// 数据库必须信息
	//err = g.Model("zy_trader_cookie").Ctx(ctx).Where("trader_id=? and is_open=?", trader[0].Id, 1).
	//	OrderDesc("update_time").Limit(1).Scan(&zyTraderCookie)
	//if nil != err {
	//	return err
	//}
	//if 0 >= len(zyTraderCookie) || 0 >= len(zyTraderCookie[0].Cookie) || 0 >= len(zyTraderCookie[0].Token) {
	//	return nil
	//}
	//
	//// 执行
	//var (
	//	retry           = false
	//	retryTimes      = 0
	//	retryTimesLimit = 5 // 重试次数
	//	cookieErr       = false
	//)
	//
	//for retryTimes < retryTimesLimit { // 最大重试
	//	//reqResData, retry, err = s.requestProxyBinancePositionHistoryNew(s.ips.Get(ipProxyUse%(s.ips.Size()-1)), traderNum)
	//	reqResData, retry, err = s.requestBinancePositionHistoryNew(traderNum, zyTraderCookie[0].Cookie, zyTraderCookie[0].Token)
	//
	//	// 需要重试
	//	if retry {
	//		retryTimes++
	//		continue
	//	}
	//
	//	// cookie不好使
	//	if 0 >= len(reqResData) {
	//		retryTimes++
	//		cookieErr = true
	//		continue
	//	} else {
	//		cookieErr = false
	//		break
	//	}
	//}
	//
	//if cookieErr {
	//	fmt.Println("新，cookie错误，信息", traderNum, reqResData)
	//	err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
	//		zyTraderCookie[0].IsOpen = 0
	//		_, err = tx.Ctx(ctx).Update("zy_trader_cookie", zyTraderCookie[0], "id", zyTraderCookie[0].Id)
	//		if nil != err {
	//			return err
	//		}
	//
	//		return nil
	//	})
	//	if nil != err {
	//		fmt.Println("新，更新数据库错误，信息", traderNum, err)
	//		return err
	//	}
	//
	//	return nil
	//}
	//
	//// 用于数据库更新
	//insertData := make([]*do.TraderPosition, 0)
	//updateData := make([]*do.TraderPosition, 0)
	//// 用于下单
	//orderInsertData := make([]*do.TraderPosition, 0)
	//orderUpdateData := make([]*do.TraderPosition, 0)
	//for _, vReqResData := range reqResData {
	//	// 新增
	//	var (
	//		currentAmount    float64
	//		currentAmountAbs float64
	//	)
	//	currentAmount, err = strconv.ParseFloat(vReqResData.PositionAmount, 64)
	//	if nil != err {
	//		fmt.Println("新，解析金额出错，信息", vReqResData, currentAmount, traderNum)
	//	}
	//	currentAmountAbs = math.Abs(currentAmount) // 绝对值
	//
	//	if _, ok := binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide]; !ok {
	//		if "BOTH" != vReqResData.PositionSide { // 单项持仓
	//			// 加入数据库
	//			insertData = append(insertData, &do.TraderPosition{
	//				Symbol:         vReqResData.Symbol,
	//				PositionSide:   vReqResData.PositionSide,
	//				PositionAmount: currentAmountAbs,
	//			})
	//
	//			// 下单
	//			if IsEqual(currentAmountAbs, 0) {
	//				continue
	//			}
	//
	//			orderInsertData = append(orderInsertData, &do.TraderPosition{
	//				Symbol:         vReqResData.Symbol,
	//				PositionSide:   vReqResData.PositionSide,
	//				PositionAmount: currentAmountAbs,
	//			})
	//		} else {
	//			// 加入数据库
	//			insertData = append(insertData, &do.TraderPosition{
	//				Symbol:         vReqResData.Symbol,
	//				PositionSide:   vReqResData.PositionSide,
	//				PositionAmount: currentAmount, // 正负数保持
	//			})
	//
	//			// 模拟为多空仓，下单，todo 组合式的判断应该时牢靠的
	//			var tmpPositionSide string
	//			if IsEqual(currentAmount, 0) {
	//				continue
	//			} else if math.Signbit(currentAmount) {
	//				// 模拟空
	//				tmpPositionSide = "SHORT"
	//				orderInsertData = append(orderInsertData, &do.TraderPosition{
	//					Symbol:         vReqResData.Symbol,
	//					PositionSide:   tmpPositionSide,
	//					PositionAmount: currentAmountAbs, // 变成绝对值
	//				})
	//			} else {
	//				// 模拟多
	//				tmpPositionSide = "LONG"
	//				orderInsertData = append(orderInsertData, &do.TraderPosition{
	//					Symbol:         vReqResData.Symbol,
	//					PositionSide:   tmpPositionSide,
	//					PositionAmount: currentAmountAbs, // 变成绝对值
	//				})
	//			}
	//		}
	//	} else {
	//		// 数量无变化
	//		if "BOTH" != vReqResData.PositionSide {
	//			if IsEqual(currentAmountAbs, binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount) {
	//				continue
	//			}
	//
	//			updateData = append(updateData, &do.TraderPosition{
	//				Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
	//				Symbol:         vReqResData.Symbol,
	//				PositionSide:   vReqResData.PositionSide,
	//				PositionAmount: currentAmountAbs,
	//			})
	//
	//			orderUpdateData = append(orderUpdateData, &do.TraderPosition{
	//				Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
	//				Symbol:         vReqResData.Symbol,
	//				PositionSide:   vReqResData.PositionSide,
	//				PositionAmount: currentAmountAbs,
	//			})
	//		} else {
	//			if IsEqual(currentAmount, binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount) {
	//				continue
	//			}
	//
	//			updateData = append(updateData, &do.TraderPosition{
	//				Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
	//				Symbol:         vReqResData.Symbol,
	//				PositionSide:   vReqResData.PositionSide,
	//				PositionAmount: currentAmount, // 正负数保持
	//			})
	//
	//			// 第一步：构造虚拟的上一次仓位，空或多或无
	//			// 这里修改一下历史仓位的信息，方便程序在后续的流程中使用，模拟both的positionAmount为正数时，修改仓位对应的多仓方向的数据，为负数时修改空仓位的数据，0时不处理
	//			if _, ok = binancePositionMap[vReqResData.Symbol+"SHORT"]; !ok {
	//				fmt.Println("新，缺少仓位SHORT，信息", binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide])
	//				continue
	//			}
	//			if _, ok = binancePositionMap[vReqResData.Symbol+"LONG"]; !ok {
	//				fmt.Println("新，缺少仓位LONG，信息", binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide])
	//				continue
	//			}
	//
	//			var lastPositionSide string // 上次仓位
	//			binancePositionMapCompare[vReqResData.Symbol+"SHORT"] = &entity.TraderPosition{
	//				Id:             binancePositionMapCompare[vReqResData.Symbol+"SHORT"].Id,
	//				Symbol:         binancePositionMapCompare[vReqResData.Symbol+"SHORT"].Symbol,
	//				PositionSide:   binancePositionMapCompare[vReqResData.Symbol+"SHORT"].PositionSide,
	//				PositionAmount: 0,
	//				CreatedAt:      binancePositionMapCompare[vReqResData.Symbol+"SHORT"].CreatedAt,
	//				UpdatedAt:      binancePositionMapCompare[vReqResData.Symbol+"SHORT"].UpdatedAt,
	//			}
	//			binancePositionMapCompare[vReqResData.Symbol+"LONG"] = &entity.TraderPosition{
	//				Id:             binancePositionMapCompare[vReqResData.Symbol+"LONG"].Id,
	//				Symbol:         binancePositionMapCompare[vReqResData.Symbol+"LONG"].Symbol,
	//				PositionSide:   binancePositionMapCompare[vReqResData.Symbol+"LONG"].PositionSide,
	//				PositionAmount: 0,
	//				CreatedAt:      binancePositionMapCompare[vReqResData.Symbol+"LONG"].CreatedAt,
	//				UpdatedAt:      binancePositionMapCompare[vReqResData.Symbol+"LONG"].UpdatedAt,
	//			}
	//
	//			if IsEqual(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount, 0) { // both仓为0
	//				// 认为两仓都无
	//
	//			} else if math.Signbit(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount) {
	//				lastPositionSide = "SHORT"
	//				binancePositionMapCompare[vReqResData.Symbol+"SHORT"].PositionAmount = math.Abs(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount)
	//			} else {
	//				lastPositionSide = "LONG"
	//				binancePositionMapCompare[vReqResData.Symbol+"LONG"].PositionAmount = math.Abs(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount)
	//			}
	//
	//			// 本次仓位
	//			var tmpPositionSide string
	//			if IsEqual(currentAmount, 0) { // 本次仓位是0
	//				if 0 >= len(lastPositionSide) {
	//					// 本次和上一次仓位都是0，应该不会走到这里
	//					fmt.Println("新，仓位异常逻辑，信息", binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide])
	//					continue
	//				}
	//
	//				// 仍为是一次完全平仓，仓位和上一次保持一致
	//				tmpPositionSide = lastPositionSide
	//			} else if math.Signbit(currentAmount) { // 判断有无符号
	//				// 第二步：本次仓位
	//
	//				// 上次和本次相反需要平上次
	//				if "LONG" == lastPositionSide {
	//					orderUpdateData = append(orderUpdateData, &do.TraderPosition{
	//						Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
	//						Symbol:         vReqResData.Symbol,
	//						PositionSide:   lastPositionSide,
	//						PositionAmount: float64(0),
	//					})
	//				}
	//
	//				tmpPositionSide = "SHORT"
	//			} else {
	//				// 第二步：本次仓位
	//
	//				// 上次和本次相反需要平上次
	//				if "SHORT" == lastPositionSide {
	//					orderUpdateData = append(orderUpdateData, &do.TraderPosition{
	//						Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
	//						Symbol:         vReqResData.Symbol,
	//						PositionSide:   lastPositionSide,
	//						PositionAmount: float64(0),
	//					})
	//				}
	//
	//				tmpPositionSide = "LONG"
	//			}
	//
	//			orderUpdateData = append(orderUpdateData, &do.TraderPosition{
	//				Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
	//				Symbol:         vReqResData.Symbol,
	//				PositionSide:   tmpPositionSide,
	//				PositionAmount: currentAmountAbs,
	//			})
	//		}
	//	}
	//}
	//
	//if 0 >= len(insertData) && 0 >= len(updateData) {
	//	return nil
	//}
	//
	//// 时长
	//fmt.Printf("程序拉取部分，开始 %v, 时长: %v, 交易员: %v\n", start, time.Since(start), traderNum)
	//err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
	//	batchSize := 500
	//	for i := 0; i < len(insertData); i += batchSize {
	//		end := i + batchSize
	//		if end > len(insertData) {
	//			end = len(insertData)
	//		}
	//		batch := insertData[i:end]
	//
	//		_, err = tx.Ctx(ctx).Insert("trader_position_"+strconv.FormatUint(traderNum, 10), batch)
	//		if nil != err {
	//			return err
	//		}
	//	}
	//
	//	for i := 0; i < len(updateData); i++ {
	//		_, err = tx.Ctx(ctx).Update("trader_position_"+strconv.FormatUint(traderNum, 10), updateData[i], "id", updateData[i].Id)
	//		if nil != err {
	//			return err
	//		}
	//	}
	//
	//	return nil
	//})
	//if nil != err {
	//	fmt.Println("新，更新数据库错误，信息", traderNum, err)
	//	return err
	//}
	//
	//// 推送订单，数据库已初始化仓位，新仓库
	//if 0 >= len(binancePositionMapCompare) {
	//	fmt.Println("初始化仓位成功，交易员信息", traderNum)
	//	return nil
	//}
	//
	//// 数据库必须信息
	//var (
	//	userBindTraders []*entity.NewUserBindTraderTwo
	//	users           []*entity.NewUser
	//)
	//err = g.Model("new_user_bind_trader_two").Ctx(ctx).
	//	Where("trader_id=? and status=? and init_order=?", trader[0].Id, 0, 1).
	//	Scan(&userBindTraders)
	//if nil != err {
	//	return err
	//}
	//
	//userIds := make([]uint, 0)
	//for _, vUserBindTraders := range userBindTraders {
	//	userIds = append(userIds, vUserBindTraders.UserId)
	//}
	//if 0 >= len(userIds) {
	//	fmt.Println("新，无人跟单的交易员下单，信息", traderNum)
	//	return nil
	//}
	//
	//err = g.Model("new_user").Ctx(ctx).
	//	Where("id in(?)", userIds).
	//	Where("api_status =? and bind_trader_status_tfi=? and is_dai=? and use_new_system=?", 1, 1, 1, 2).
	//	Scan(&users)
	//if nil != err {
	//	return err
	//}
	//if 0 >= len(users) {
	//	fmt.Println("新，未查询到用户信息，信息", traderNum)
	//	return nil
	//}
	//
	//// 处理
	//usersMap := make(map[uint]*entity.NewUser, 0)
	//for _, vUsers := range users {
	//	usersMap[vUsers.Id] = vUsers
	//}
	//
	//// 获取代币信息
	//var (
	//	symbols []*entity.LhCoinSymbol
	//)
	//err = g.Model("lh_coin_symbol").Ctx(ctx).Scan(&symbols)
	//if nil != err {
	//	return err
	//}
	//if 0 >= len(symbols) {
	//	fmt.Println("新，空的代币信息，信息", traderNum)
	//	return nil
	//}
	//
	//// 处理
	//symbolsMap := make(map[string]*entity.LhCoinSymbol, 0)
	//for _, vSymbols := range symbols {
	//	symbolsMap[vSymbols.Symbol+"USDT"] = vSymbols
	//}
	//
	//// 获取交易员和用户的最新保证金信息
	//var (
	//	userInfos   []*entity.NewUserInfo
	//	traderInfos []*entity.NewTraderInfo
	//)
	//err = g.Model("new_user_info").Ctx(ctx).Scan(&userInfos)
	//if nil != err {
	//	return err
	//}
	//// 处理
	//userInfosMap := make(map[uint]*entity.NewUserInfo, 0)
	//for _, vUserInfos := range userInfos {
	//	userInfosMap[vUserInfos.UserId] = vUserInfos
	//}
	//
	//err = g.Model("new_trader_info").Ctx(ctx).Scan(&traderInfos)
	//if nil != err {
	//	return err
	//}
	//// 处理
	//traderInfosMap := make(map[uint]*entity.NewTraderInfo, 0)
	//for _, vTraderInfos := range traderInfos {
	//	traderInfosMap[vTraderInfos.TraderId] = vTraderInfos
	//}
	//
	//wg := sync.WaitGroup{}
	//// 遍历跟单者
	//for _, vUserBindTraders := range userBindTraders {
	//	tmpUserBindTraders := vUserBindTraders
	//	if _, ok := usersMap[tmpUserBindTraders.UserId]; !ok {
	//		fmt.Println("新，未匹配到用户信息，用户的信息无效了，信息", traderNum, tmpUserBindTraders)
	//		continue
	//	}
	//
	//	tmpTrader := trader[0]
	//	tmpUsers := usersMap[tmpUserBindTraders.UserId]
	//	if 0 >= len(tmpUsers.ApiSecret) || 0 >= len(tmpUsers.ApiKey) {
	//		fmt.Println("新，用户的信息无效了，信息", traderNum, tmpUserBindTraders)
	//		continue
	//	}
	//
	//	if lessThanOrEqualZero(tmpTrader.BaseMoney, 0, 1e-7) {
	//		fmt.Println("新，交易员信息无效了，信息", tmpTrader, tmpUserBindTraders)
	//		continue
	//	}
	//
	//	// 新增仓位
	//	for _, vInsertData := range orderInsertData {
	//		// 一个新symbol通常3个开仓方向short，long，both，屏蔽一下未真实开仓的
	//		tmpInsertData := vInsertData
	//		if lessThanOrEqualZero(tmpInsertData.PositionAmount.(float64), 0, 1e-7) {
	//			continue
	//		}
	//
	//		if _, ok := symbolsMap[tmpInsertData.Symbol.(string)]; !ok {
	//			fmt.Println("新，代币信息无效，信息", tmpInsertData, tmpUserBindTraders)
	//			continue
	//		}
	//
	//		var (
	//			tmpQty        float64
	//			quantity      string
	//			quantityFloat float64
	//			side          string
	//			positionSide  string
	//			orderType     = "MARKET"
	//		)
	//		if "LONG" == tmpInsertData.PositionSide {
	//			positionSide = "LONG"
	//			side = "BUY"
	//		} else if "SHORT" == tmpInsertData.PositionSide {
	//			positionSide = "SHORT"
	//			side = "SELL"
	//		} else {
	//			fmt.Println("新，无效信息，信息", vInsertData)
	//			continue
	//		}
	//
	//		// 本次 代单员币的数量 * (用户保证金/代单员保证金)
	//		tmpQty = tmpInsertData.PositionAmount.(float64) * float64(tmpUserBindTraders.Amount) / tmpTrader.BaseMoney // 本次开单数量
	//
	//		// todo 目前是松柏系列账户
	//		if _, ok := userInfosMap[tmpUserBindTraders.UserId]; ok {
	//			if _, ok2 := traderInfosMap[tmpUserBindTraders.TraderId]; ok2 {
	//				if 0 < traderInfosMap[tmpUserBindTraders.TraderId].BaseMoney && 0 < userInfosMap[tmpUserBindTraders.UserId].BaseMoney {
	//					tmpQty = tmpInsertData.PositionAmount.(float64) * userInfosMap[tmpUserBindTraders.UserId].BaseMoney / traderInfosMap[tmpUserBindTraders.TraderId].BaseMoney // 本次开单数量
	//				} else {
	//					fmt.Println("新，无效信息base_money1，信息", traderInfosMap[tmpUserBindTraders.TraderId], userInfosMap[tmpUserBindTraders.UserId])
	//				}
	//			}
	//		}
	//
	//		// 精度调整
	//		if 0 >= symbolsMap[tmpInsertData.Symbol.(string)].QuantityPrecision {
	//			quantity = fmt.Sprintf("%d", int64(tmpQty))
	//		} else {
	//			quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap[tmpInsertData.Symbol.(string)].QuantityPrecision, 64)
	//		}
	//
	//		quantityFloat, err = strconv.ParseFloat(quantity, 64)
	//		if nil != err {
	//			fmt.Println(err)
	//			return
	//		}
	//
	//		if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
	//			continue
	//		}
	//
	//		wg.Add(1)
	//		err = s.pool.Add(ctx, func(ctx context.Context) {
	//			defer wg.Done()
	//
	//			// 下单，不用计算数量，新仓位
	//			// 新订单数据
	//			currentOrder := &do.NewUserOrderTwo{
	//				UserId:        tmpUserBindTraders.UserId,
	//				TraderId:      tmpUserBindTraders.TraderId,
	//				Symbol:        tmpInsertData.Symbol.(string),
	//				Side:          side,
	//				PositionSide:  positionSide,
	//				Quantity:      quantityFloat,
	//				Price:         0,
	//				TraderQty:     tmpInsertData.PositionAmount.(float64),
	//				OrderType:     orderType,
	//				ClosePosition: "",
	//				CumQuote:      0,
	//				ExecutedQty:   0,
	//				AvgPrice:      0,
	//			}
	//
	//			var (
	//				binanceOrderRes *binanceOrder
	//				orderInfoRes    *orderInfo
	//			)
	//			// 请求下单
	//			binanceOrderRes, orderInfoRes, err = requestBinanceOrder(tmpInsertData.Symbol.(string), side, orderType, positionSide, quantity, tmpUsers.ApiKey, tmpUsers.ApiSecret)
	//			if nil != err {
	//				fmt.Println(err)
	//				return
	//			}
	//
	//			// 下单异常
	//			if 0 >= binanceOrderRes.OrderId {
	//				// 写入
	//				orderErr := &do.NewUserOrderErrTwo{
	//					UserId:        currentOrder.UserId,
	//					TraderId:      currentOrder.TraderId,
	//					ClientOrderId: "",
	//					OrderId:       "",
	//					Symbol:        currentOrder.Symbol,
	//					Side:          currentOrder.Side,
	//					PositionSide:  currentOrder.PositionSide,
	//					Quantity:      quantityFloat,
	//					Price:         currentOrder.Price,
	//					TraderQty:     currentOrder.TraderQty,
	//					OrderType:     currentOrder.OrderType,
	//					ClosePosition: currentOrder.ClosePosition,
	//					CumQuote:      currentOrder.CumQuote,
	//					ExecutedQty:   currentOrder.ExecutedQty,
	//					AvgPrice:      currentOrder.AvgPrice,
	//					HandleStatus:  currentOrder.HandleStatus,
	//					Code:          orderInfoRes.Code,
	//					Msg:           orderInfoRes.Msg,
	//					Proportion:    0,
	//				}
	//
	//				err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
	//					_, err = tx.Ctx(ctx).Insert("new_user_order_err_two", orderErr)
	//					if nil != err {
	//						fmt.Println(err)
	//						return err
	//					}
	//
	//					return nil
	//				})
	//				if nil != err {
	//					fmt.Println("新，下单错误，记录错误信息错误，信息", err, traderNum, vInsertData, tmpUserBindTraders)
	//					return
	//				}
	//
	//				return // 返回
	//			}
	//
	//			currentOrder.OrderId = strconv.FormatInt(binanceOrderRes.OrderId, 10)
	//
	//			currentOrder.CumQuote, err = strconv.ParseFloat(binanceOrderRes.CumQuote, 64)
	//			if nil != err {
	//				fmt.Println("新，下单错误，解析错误1，信息", err, currentOrder, binanceOrderRes)
	//				return
	//			}
	//
	//			currentOrder.ExecutedQty, err = strconv.ParseFloat(binanceOrderRes.ExecutedQty, 64)
	//			if nil != err {
	//				fmt.Println("新，下单错误，解析错误2，信息", err, currentOrder, binanceOrderRes)
	//				return
	//			}
	//
	//			currentOrder.AvgPrice, err = strconv.ParseFloat(binanceOrderRes.AvgPrice, 64)
	//			if nil != err {
	//				fmt.Println("新，下单错误，解析错误3，信息", err, currentOrder, binanceOrderRes)
	//				return
	//			}
	//
	//			// 写入
	//			err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
	//				_, err = tx.Ctx(ctx).Insert("new_user_order_two_"+strconv.FormatInt(int64(tmpUserBindTraders.UserId), 10), currentOrder)
	//				if nil != err {
	//					fmt.Println(err)
	//					return err
	//				}
	//
	//				return nil
	//			})
	//			if nil != err {
	//				fmt.Println("新，下单错误，记录信息错误，信息", err, traderNum, vInsertData, tmpUserBindTraders)
	//				return
	//			}
	//
	//			return
	//		})
	//		if nil != err {
	//			fmt.Println("新，添加下单任务异常，新增仓位，错误信息：", err, traderNum, vInsertData, tmpUserBindTraders)
	//		}
	//	}
	//
	//	// 判断需要修改仓位
	//	if 0 >= len(orderUpdateData) {
	//		continue
	//	}
	//
	//	// 获取用户历史仓位
	//	var (
	//		userOrderTwo []*entity.NewUserOrderTwo
	//	)
	//	err = g.Model("new_user_order_two_"+strconv.FormatInt(int64(tmpUserBindTraders.UserId), 10)).Ctx(ctx).
	//		Where("trader_id=?", tmpTrader.Id).
	//		Scan(&userOrderTwo)
	//	if nil != err {
	//		return err
	//	}
	//	userOrderTwoSymbolPositionSideCount := make(map[string]float64, 0)
	//	for _, vUserOrderTwo := range userOrderTwo {
	//		if _, ok := userOrderTwoSymbolPositionSideCount[vUserOrderTwo.Symbol+vUserOrderTwo.PositionSide]; !ok {
	//			userOrderTwoSymbolPositionSideCount[vUserOrderTwo.Symbol+vUserOrderTwo.PositionSide] = 0
	//		}
	//
	//		if "LONG" == vUserOrderTwo.PositionSide {
	//			if "BUY" == vUserOrderTwo.Side {
	//				userOrderTwoSymbolPositionSideCount[vUserOrderTwo.Symbol+vUserOrderTwo.PositionSide] += vUserOrderTwo.ExecutedQty
	//			} else if "SELL" == vUserOrderTwo.Side {
	//				userOrderTwoSymbolPositionSideCount[vUserOrderTwo.Symbol+vUserOrderTwo.PositionSide] -= vUserOrderTwo.ExecutedQty
	//			} else {
	//				fmt.Println("新，历史仓位解析异常1，错误信息：", err, traderNum, vUserOrderTwo, tmpUserBindTraders)
	//				continue
	//			}
	//		} else if "SHORT" == vUserOrderTwo.PositionSide {
	//			if "SELL" == vUserOrderTwo.Side {
	//				userOrderTwoSymbolPositionSideCount[vUserOrderTwo.Symbol+vUserOrderTwo.PositionSide] += vUserOrderTwo.ExecutedQty
	//			} else if "BUY" == vUserOrderTwo.Side {
	//				userOrderTwoSymbolPositionSideCount[vUserOrderTwo.Symbol+vUserOrderTwo.PositionSide] -= vUserOrderTwo.ExecutedQty
	//			} else {
	//				fmt.Println("新，历史仓位解析异常2，错误信息：", err, traderNum, vUserOrderTwo, tmpUserBindTraders)
	//				continue
	//			}
	//		} else {
	//			fmt.Println("新，历史仓位解析异常3，错误信息：", err, traderNum, vUserOrderTwo, tmpUserBindTraders)
	//			continue
	//		}
	//	}
	//
	//	// 修改仓位
	//	for _, vUpdateData := range orderUpdateData {
	//		tmpUpdateData := vUpdateData
	//		if _, ok := binancePositionMapCompare[vUpdateData.Symbol.(string)+vUpdateData.PositionSide.(string)]; !ok {
	//			fmt.Println("新，添加下单任务异常，修改仓位，错误信息：", err, traderNum, vUpdateData, tmpUserBindTraders)
	//			continue
	//		}
	//		lastPositionData := binancePositionMapCompare[vUpdateData.Symbol.(string)+vUpdateData.PositionSide.(string)]
	//
	//		if _, ok := symbolsMap[tmpUpdateData.Symbol.(string)]; !ok {
	//			fmt.Println("新，代币信息无效，信息", tmpUpdateData, tmpUserBindTraders)
	//			continue
	//		}
	//
	//		var (
	//			tmpQty        float64
	//			quantity      string
	//			quantityFloat float64
	//			side          string
	//			positionSide  string
	//			orderType     = "MARKET"
	//		)
	//
	//		if lessThanOrEqualZero(tmpUpdateData.PositionAmount.(float64), 0, 1e-7) {
	//			fmt.Println("新，完全平仓：", tmpUpdateData)
	//			// 全平仓
	//			if "LONG" == tmpUpdateData.PositionSide {
	//				positionSide = "LONG"
	//				side = "SELL"
	//			} else if "SHORT" == tmpUpdateData.PositionSide {
	//				positionSide = "SHORT"
	//				side = "BUY"
	//			} else {
	//				fmt.Println("新，无效信息，信息", tmpUpdateData)
	//				continue
	//			}
	//
	//			// 未开启过仓位
	//			if _, ok := userOrderTwoSymbolPositionSideCount[tmpUpdateData.Symbol.(string)+tmpUpdateData.PositionSide.(string)]; !ok {
	//				continue
	//			}
	//
	//			// 认为是0
	//			if lessThanOrEqualZero(userOrderTwoSymbolPositionSideCount[tmpUpdateData.Symbol.(string)+tmpUpdateData.PositionSide.(string)], 0, 1e-7) {
	//				continue
	//			}
	//
	//			// 剩余仓位
	//			tmpQty = userOrderTwoSymbolPositionSideCount[tmpUpdateData.Symbol.(string)+tmpUpdateData.PositionSide.(string)]
	//		} else if lessThanOrEqualZero(lastPositionData.PositionAmount, tmpUpdateData.PositionAmount.(float64), 1e-7) {
	//			fmt.Println("新，追加仓位：", tmpUpdateData, lastPositionData)
	//			// 本次加仓 代单员币的数量 * (用户保证金/代单员保证金)
	//			if "LONG" == tmpUpdateData.PositionSide {
	//				positionSide = "LONG"
	//				side = "BUY"
	//			} else if "SHORT" == tmpUpdateData.PositionSide {
	//				positionSide = "SHORT"
	//				side = "SELL"
	//			} else {
	//				fmt.Println("新，无效信息，信息", tmpUpdateData)
	//				continue
	//			}
	//
	//			// 本次减去上一次
	//			tmpQty = (tmpUpdateData.PositionAmount.(float64) - lastPositionData.PositionAmount) * float64(tmpUserBindTraders.Amount) / tmpTrader.BaseMoney // 本次开单数量
	//			if _, ok := userInfosMap[tmpUserBindTraders.UserId]; ok {
	//				if _, ok2 := traderInfosMap[tmpUserBindTraders.TraderId]; ok2 {
	//					if 0 < traderInfosMap[tmpUserBindTraders.TraderId].BaseMoney && 0 < userInfosMap[tmpUserBindTraders.UserId].BaseMoney {
	//						tmpQty = (tmpUpdateData.PositionAmount.(float64) - lastPositionData.PositionAmount) * userInfosMap[tmpUserBindTraders.UserId].BaseMoney / traderInfosMap[tmpUserBindTraders.TraderId].BaseMoney // 本次开单数量
	//					} else {
	//						fmt.Println("新，无效信息base_money2，信息", traderInfosMap[tmpUserBindTraders.TraderId], userInfosMap[tmpUserBindTraders.UserId])
	//					}
	//				}
	//			}
	//
	//		} else if lessThanOrEqualZero(tmpUpdateData.PositionAmount.(float64), lastPositionData.PositionAmount, 1e-7) {
	//			fmt.Println("新，部分平仓：", tmpUpdateData, lastPositionData)
	//			// 部分平仓
	//			if "LONG" == tmpUpdateData.PositionSide {
	//				positionSide = "LONG"
	//				side = "SELL"
	//			} else if "SHORT" == tmpUpdateData.PositionSide {
	//				positionSide = "SHORT"
	//				side = "BUY"
	//			} else {
	//				fmt.Println("新，无效信息，信息", tmpUpdateData)
	//				continue
	//			}
	//
	//			// 未开启过仓位
	//			if _, ok := userOrderTwoSymbolPositionSideCount[tmpUpdateData.Symbol.(string)+tmpUpdateData.PositionSide.(string)]; !ok {
	//				continue
	//			}
	//
	//			// 认为是0
	//			if lessThanOrEqualZero(userOrderTwoSymbolPositionSideCount[tmpUpdateData.Symbol.(string)+tmpUpdateData.PositionSide.(string)], 0, 1e-7) {
	//				continue
	//			}
	//
	//			// 上次仓位
	//			if lessThanOrEqualZero(lastPositionData.PositionAmount, 0, 1e-7) {
	//				fmt.Println("新，部分平仓，上次仓位信息无效，信息", lastPositionData, tmpUpdateData)
	//				continue
	//			}
	//
	//			// 按百分比
	//			tmpQty = userOrderTwoSymbolPositionSideCount[tmpUpdateData.Symbol.(string)+tmpUpdateData.PositionSide.(string)] * (lastPositionData.PositionAmount - tmpUpdateData.PositionAmount.(float64)) / lastPositionData.PositionAmount
	//		} else {
	//			fmt.Println("新，分析仓位无效，信息", lastPositionData, tmpUpdateData)
	//			continue
	//		}
	//
	//		// 精度调整
	//		if 0 >= symbolsMap[tmpUpdateData.Symbol.(string)].QuantityPrecision {
	//			quantity = fmt.Sprintf("%d", int64(tmpQty))
	//		} else {
	//			quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap[tmpUpdateData.Symbol.(string)].QuantityPrecision, 64)
	//		}
	//
	//		quantityFloat, err = strconv.ParseFloat(quantity, 64)
	//		if nil != err {
	//			fmt.Println(err)
	//			return
	//		}
	//
	//		if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
	//			continue
	//		}
	//
	//		fmt.Println("新，下单数字，信息:", quantity, quantityFloat)
	//
	//		wg.Add(1)
	//		err = s.pool.Add(ctx, func(ctx context.Context) {
	//			defer wg.Done()
	//
	//			// 下单，不用计算数量，新仓位
	//			// 新订单数据
	//			currentOrder := &do.NewUserOrderTwo{
	//				UserId:        tmpUserBindTraders.UserId,
	//				TraderId:      tmpUserBindTraders.TraderId,
	//				Symbol:        tmpUpdateData.Symbol.(string),
	//				Side:          side,
	//				PositionSide:  positionSide,
	//				Quantity:      quantityFloat,
	//				Price:         0,
	//				TraderQty:     quantityFloat,
	//				OrderType:     orderType,
	//				ClosePosition: "",
	//				CumQuote:      0,
	//				ExecutedQty:   0,
	//				AvgPrice:      0,
	//			}
	//
	//			var (
	//				binanceOrderRes *binanceOrder
	//				orderInfoRes    *orderInfo
	//			)
	//			// 请求下单
	//			binanceOrderRes, orderInfoRes, err = requestBinanceOrder(tmpUpdateData.Symbol.(string), side, orderType, positionSide, quantity, tmpUsers.ApiKey, tmpUsers.ApiSecret)
	//			if nil != err {
	//				fmt.Println(err)
	//				return
	//			}
	//
	//			// 下单异常
	//			if 0 >= binanceOrderRes.OrderId {
	//				// 写入
	//				orderErr := &do.NewUserOrderErrTwo{
	//					UserId:        currentOrder.UserId,
	//					TraderId:      currentOrder.TraderId,
	//					ClientOrderId: "",
	//					OrderId:       "",
	//					Symbol:        currentOrder.Symbol,
	//					Side:          currentOrder.Side,
	//					PositionSide:  currentOrder.PositionSide,
	//					Quantity:      quantityFloat,
	//					Price:         currentOrder.Price,
	//					TraderQty:     currentOrder.TraderQty,
	//					OrderType:     currentOrder.OrderType,
	//					ClosePosition: currentOrder.ClosePosition,
	//					CumQuote:      currentOrder.CumQuote,
	//					ExecutedQty:   currentOrder.ExecutedQty,
	//					AvgPrice:      currentOrder.AvgPrice,
	//					HandleStatus:  currentOrder.HandleStatus,
	//					Code:          orderInfoRes.Code,
	//					Msg:           orderInfoRes.Msg,
	//					Proportion:    0,
	//				}
	//
	//				err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
	//					_, err = tx.Ctx(ctx).Insert("new_user_order_err_two", orderErr)
	//					if nil != err {
	//						fmt.Println(err)
	//						return err
	//					}
	//
	//					return nil
	//				})
	//				if nil != err {
	//					fmt.Println("新，下单错误，记录错误信息错误，信息", err, traderNum, tmpUpdateData, tmpUserBindTraders)
	//					return
	//				}
	//
	//				return // 返回
	//			}
	//
	//			currentOrder.OrderId = strconv.FormatInt(binanceOrderRes.OrderId, 10)
	//
	//			currentOrder.CumQuote, err = strconv.ParseFloat(binanceOrderRes.CumQuote, 64)
	//			if nil != err {
	//				fmt.Println("新，下单错误，解析错误1，信息", err, currentOrder, binanceOrderRes)
	//				return
	//			}
	//
	//			currentOrder.ExecutedQty, err = strconv.ParseFloat(binanceOrderRes.ExecutedQty, 64)
	//			if nil != err {
	//				fmt.Println("新，下单错误，解析错误2，信息", err, currentOrder, binanceOrderRes)
	//				return
	//			}
	//
	//			currentOrder.AvgPrice, err = strconv.ParseFloat(binanceOrderRes.AvgPrice, 64)
	//			if nil != err {
	//				fmt.Println("新，下单错误，解析错误3，信息", err, currentOrder, binanceOrderRes)
	//				return
	//			}
	//
	//			// 写入
	//			err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
	//				_, err = tx.Ctx(ctx).Insert("new_user_order_two_"+strconv.FormatInt(int64(tmpUserBindTraders.UserId), 10), currentOrder)
	//				if nil != err {
	//					fmt.Println(err)
	//					return err
	//				}
	//
	//				return nil
	//			})
	//			if nil != err {
	//				fmt.Println("新，下单错误，记录信息错误，信息", err, traderNum, tmpUpdateData, tmpUserBindTraders)
	//				return
	//			}
	//		})
	//		if nil != err {
	//			fmt.Println("新，添加下单任务异常，修改仓位，错误信息：", err, traderNum, vUpdateData, tmpUserBindTraders)
	//		}
	//	}
	//
	//}
	//// 回收协程
	//wg.Wait()

	return nil
}

var (
	globalTraderNum = uint64(3887627985594221568) // todo 改 3887627985594221568
	orderMap        = gmap.New(true)              // 初始化下单记录
	orderErr        = gset.New(true)

	baseMoneyGuiTu      = gtype.NewFloat64()
	baseMoneyUserAllMap = gmap.NewIntAnyMap(true)

	globalUsers = gmap.New(true)

	// 仓位
	binancePositionMap = make(map[string]*entity.TraderPosition, 0)

	symbolsMap = gmap.NewStrAnyMap(true)
)

// GetGlobalInfo 获取全局测试数据
func (s *sBinanceTraderHistory) GetGlobalInfo(ctx context.Context) {
	// 遍历map
	orderMap.Iterator(func(k interface{}, v interface{}) bool {
		fmt.Println("龟兔，用户下单，测试结果:", k, v)
		return true
	})

	orderErr.Iterator(func(v interface{}) bool {
		fmt.Println("龟兔，用户下单，测试结果，错误单:", v)
		return true
	})

	fmt.Println("龟兔，保证金，测试结果:", baseMoneyGuiTu)
	baseMoneyUserAllMap.Iterator(func(k int, v interface{}) bool {
		fmt.Println("龟兔，保证金，用户，测试结果:", k, v)
		return true
	})

	globalUsers.Iterator(func(k interface{}, v interface{}) bool {
		fmt.Println("龟兔，用户信息:", v.(*entity.NewUser))
		return true
	})

	for _, vBinancePositionMap := range binancePositionMap {
		if IsEqual(vBinancePositionMap.PositionAmount, 0) {
			continue
		}

		fmt.Println("龟兔，带单员仓位，信息:", vBinancePositionMap)
	}
}

//// InitGlobalInfo 初始化信息
//func (s *sBinanceTraderHistory) InitGlobalInfo(ctx context.Context) bool {
//	var (
//		err          error
//		keyPositions []*entity.KeyPosition
//	)
//	err = g.Model("key_position").Ctx(ctx).Where("amount>", 0).Scan(&keyPositions)
//	if nil != err {
//		fmt.Println("龟兔，初始化仓位，数据库查询错误：", err)
//		return false
//	}
//
//	for _, vKeyPositions := range keyPositions {
//		orderMap.Set(vKeyPositions.Key, vKeyPositions.Amount)
//	}
//
//	return true
//}

// UpdateCoinInfo 初始化信息
func (s *sBinanceTraderHistory) UpdateCoinInfo(ctx context.Context) bool {
	// 获取代币信息
	var (
		err     error
		symbols []*entity.LhCoinSymbol
	)
	err = g.Model("lh_coin_symbol").Ctx(ctx).Scan(&symbols)
	if nil != err || 0 >= len(symbols) {
		fmt.Println("龟兔，初始化，币种，数据库查询错误：", err)
		return false
	}
	// 处理
	for _, vSymbols := range symbols {
		symbolsMap.Set(vSymbols.Symbol+"USDT", vSymbols)
	}

	return true
}

// UpdateKeyPosition 更新keyPosition信息
func (s *sBinanceTraderHistory) UpdateKeyPosition(ctx context.Context) bool {
	var (
		err error
	)

	keyPositionsNew := make(map[string]float64, 0)
	orderMap.Iterator(func(k interface{}, v interface{}) bool {
		keyPositionsNew[k.(string)] = v.(float64)
		return true
	})

	if 0 >= len(keyPositionsNew) {
		return true
	}

	var (
		keyPositions []*entity.KeyPosition
	)
	err = g.Model("key_position").Ctx(ctx).Scan(&keyPositions)
	if nil != err {
		fmt.Println("龟兔，初始化仓位，数据库查询错误：", err)
		return true
	}

	keyPositionsMap := make(map[string]*entity.KeyPosition, 0)
	for _, vKeyPositions := range keyPositions {
		keyPositionsMap[vKeyPositions.Key] = vKeyPositions
	}

	_, err = g.Model("key_position").Ctx(ctx).Where("amount>", 0).Data("amount", 0).Update()
	if nil != err {
		fmt.Println("龟兔，key_position，数据库清0：", err)
	}

	for k, v := range keyPositionsNew {
		if _, ok := keyPositionsMap[k]; ok {
			_, err = g.Model("key_position").Ctx(ctx).Data("amount", v).Where("key", k).Update()
			if nil != err {
				fmt.Println("龟兔，key_position，数据库更新：", err)
			}
		} else {
			_, err = g.Model("key_position").Ctx(ctx).Insert(&do.KeyPosition{
				Key:    k,
				Amount: v,
			})

			if nil != err {
				fmt.Println("龟兔，key_position，数据库写入：", err)
			}
		}
	}

	return true
}

// InitGlobalInfo 初始化信息
func (s *sBinanceTraderHistory) InitGlobalInfo(ctx context.Context) bool {
	// 获取代币信息
	var (
		err error
	)

	// 初始化，恢复仓位数据
	var (
		users []*entity.NewUser
	)
	err = g.Model("new_user").Ctx(ctx).
		Where("api_status =? and bind_trader_status_tfi=? and is_dai=? and use_new_system=?", 1, 1, 1, 2).
		Scan(&users)
	if nil != err {
		fmt.Println("龟兔，初始化，数据库查询错误：", err)
		return false
	}

	// 执行
	var (
		retry           = false
		retryTimes      = 0
		retryTimesLimit = 5 // 重试次数
		cookieErr       bool
		reqResData      []*binancePositionDataList
	)

	for _, vUsers := range users {
		strUserId := strconv.FormatUint(uint64(vUsers.Id), 10)

		for retryTimes < retryTimesLimit { // 最大重试
			// 龟兔的数据
			reqResData, retry, _ = s.requestBinancePositionHistoryNew(uint64(vUsers.BinanceId), "", "")

			// 需要重试
			if retry {
				retryTimes++
				time.Sleep(time.Second * 5)
				fmt.Println("龟兔，重试：", retry)
				continue
			}

			// cookie不好使
			if 0 >= len(reqResData) {
				retryTimes++
				cookieErr = true
				continue
			} else {
				cookieErr = false
				break
			}
		}

		// cookie 错误
		if cookieErr {
			fmt.Println("龟兔，初始化，查询仓位，cookie错误")
			return false
		}

		for _, vReqResData := range reqResData {
			// 新增
			var (
				currentAmount    float64
				currentAmountAbs float64
			)
			currentAmount, err = strconv.ParseFloat(vReqResData.PositionAmount, 64)
			if nil != err {
				fmt.Println("龟兔，初始化，解析金额出错，信息", vReqResData, currentAmount)
				return false
			}
			currentAmountAbs = math.Abs(currentAmount) // 绝对值

			if lessThanOrEqualZero(currentAmountAbs, 0, 1e-7) {
				continue
			}

			fmt.Println("龟兔，初始化，用户现在的仓位，信息", vReqResData.Symbol+vReqResData.PositionSide+strUserId, currentAmountAbs)
			orderMap.Set(vReqResData.Symbol+"&"+vReqResData.PositionSide+"&"+strUserId, currentAmountAbs)
		}

		time.Sleep(30 * time.Millisecond)
	}

	return true
}

// PullAndSetBaseMoneyNewGuiTuAndUser 拉取binance保证金数据
func (s *sBinanceTraderHistory) PullAndSetBaseMoneyNewGuiTuAndUser(ctx context.Context) {
	var (
		err error
		one string
	)

	one, err = requestBinanceTraderDetail(globalTraderNum)
	if nil != err {
		fmt.Println("龟兔，拉取保证金失败：", err, globalTraderNum)
	}
	if 0 < len(one) {
		var tmp float64
		tmp, err = strconv.ParseFloat(one, 64)
		if nil != err {
			fmt.Println("龟兔，拉取保证金，转化失败：", err, globalTraderNum)
		}

		if !IsEqual(tmp, baseMoneyGuiTu.Val()) {
			fmt.Println("龟兔，变更保证金")
			baseMoneyGuiTu.Set(tmp)
		}
	}
	time.Sleep(300 * time.Millisecond)

	var (
		users []*entity.NewUser
	)
	err = g.Model("new_user").Ctx(ctx).
		Where("api_status =? and bind_trader_status_tfi=? and is_dai=? and use_new_system=?", 1, 1, 1, 2).
		Scan(&users)
	if nil != err {
		fmt.Println("龟兔，新增用户，数据库查询错误：", err)
		return
	}

	tmpUserMap := make(map[uint]*entity.NewUser, 0)
	for _, vUsers := range users {
		tmpUserMap[vUsers.Id] = vUsers
	}

	globalUsers.Iterator(func(k interface{}, v interface{}) bool {
		vGlobalUsers := v.(*entity.NewUser)

		if _, ok := tmpUserMap[vGlobalUsers.Id]; !ok {
			fmt.Println("龟兔，变更保证金，用户数据错误，数据库不存在：", vGlobalUsers)
			return true
		}

		if 0 >= vGlobalUsers.BinanceId {
			fmt.Println("龟兔，变更保证金，用户数据错误：", vGlobalUsers)
			return true
		}

		var (
			detail string
		)
		detail, err = requestBinanceTraderDetail(uint64(vGlobalUsers.BinanceId))
		if nil != err {
			fmt.Println("龟兔，拉取保证金失败：", err, vGlobalUsers)
			return true
		}
		if 0 < len(detail) {
			var tmp float64
			tmp, err = strconv.ParseFloat(detail, 64)
			if nil != err {
				fmt.Println("龟兔，拉取保证金，转化失败：", err, vGlobalUsers)
				return true
			}

			tmp *= tmpUserMap[vGlobalUsers.Id].Num
			if !baseMoneyUserAllMap.Contains(int(vGlobalUsers.Id)) {
				fmt.Println("初始化成功保证金", vGlobalUsers, tmp, tmpUserMap[vGlobalUsers.Id].Num)
				baseMoneyUserAllMap.Set(int(vGlobalUsers.Id), tmp)
			} else {
				if !IsEqual(tmp, baseMoneyUserAllMap.Get(int(vGlobalUsers.Id)).(float64)) {
					fmt.Println("变更成功", int(vGlobalUsers.Id), tmp, tmpUserMap[vGlobalUsers.Id].Num)
					baseMoneyUserAllMap.Set(int(vGlobalUsers.Id), tmp)
				}
			}
		}

		time.Sleep(300 * time.Millisecond)
		return true
	})
}

// InsertGlobalUsers  新增用户
func (s *sBinanceTraderHistory) InsertGlobalUsers(ctx context.Context) {
	var (
		err   error
		users []*entity.NewUser
	)
	err = g.Model("new_user").Ctx(ctx).
		Where("api_status =? and bind_trader_status_tfi=? and is_dai=? and use_new_system=?", 1, 1, 1, 2).
		Scan(&users)
	if nil != err {
		fmt.Println("龟兔，新增用户，数据库查询错误：", err)
		return
	}

	tmpUserMap := make(map[uint]*entity.NewUser, 0)
	for _, vUsers := range users {
		tmpUserMap[vUsers.Id] = vUsers
	}

	// 第一遍比较，新增
	for k, vTmpUserMap := range tmpUserMap {
		if globalUsers.Contains(k) {
			continue
		}

		// 初始化仓位
		fmt.Println("龟兔，新增用户:", k, vTmpUserMap)
		if 1 == vTmpUserMap.NeedInit {
			_, err = g.Model("new_user").Ctx(ctx).Data("need_init", 0).Where("id=?", vTmpUserMap.Id).Update()
			if nil != err {
				fmt.Println("龟兔，新增用户，更新初始化状态失败:", k, vTmpUserMap)
			}

			strUserId := strconv.FormatUint(uint64(vTmpUserMap.Id), 10)

			if lessThanOrEqualZero(vTmpUserMap.Num, 0, 1e-7) {
				fmt.Println("龟兔，新增用户，保证金系数错误：", vTmpUserMap)
				continue
			}

			// 新增仓位
			tmpTraderBaseMoney := baseMoneyGuiTu.Val()
			if lessThanOrEqualZero(tmpTraderBaseMoney, 0, 1e-7) {
				fmt.Println("龟兔，新增用户，交易员信息无效了，信息", vTmpUserMap)
				continue
			}

			// 获取保证金
			var tmpUserBindTradersAmount float64

			var (
				detail string
			)
			detail, err = requestBinanceTraderDetail(uint64(vTmpUserMap.BinanceId))
			if nil != err {
				fmt.Println("龟兔，新增用户，拉取保证金失败：", err, vTmpUserMap)
			}
			if 0 < len(detail) {
				var tmp float64
				tmp, err = strconv.ParseFloat(detail, 64)
				if nil != err {
					fmt.Println("龟兔，新增用户，拉取保证金，转化失败：", err, vTmpUserMap)
				}

				tmp *= vTmpUserMap.Num
				tmpUserBindTradersAmount = tmp
				if !baseMoneyUserAllMap.Contains(int(vTmpUserMap.Id)) {
					fmt.Println("新增用户，初始化成功保证金", vTmpUserMap, tmp, vTmpUserMap.Num)
					baseMoneyUserAllMap.Set(int(vTmpUserMap.Id), tmp)
				} else {
					if !IsEqual(tmp, baseMoneyUserAllMap.Get(int(vTmpUserMap.Id)).(float64)) {
						fmt.Println("新增用户，变更成功", int(vTmpUserMap.Id), tmp, vTmpUserMap.Num)
						baseMoneyUserAllMap.Set(int(vTmpUserMap.Id), tmp)
					}
				}
			}

			if lessThanOrEqualZero(tmpUserBindTradersAmount, 0, 1e-7) {
				fmt.Println("龟兔，新增用户，保证金不足为0：", tmpUserBindTradersAmount, vTmpUserMap.Id)
				continue
			}

			// 仓位
			for _, vInsertData := range binancePositionMap {
				// 一个新symbol通常3个开仓方向short，long，both，屏蔽一下未真实开仓的
				tmpInsertData := vInsertData
				if IsEqual(tmpInsertData.PositionAmount, 0) {
					continue
				}

				if !symbolsMap.Contains(tmpInsertData.Symbol) {
					fmt.Println("龟兔，新增用户，代币信息无效，信息", tmpInsertData, vTmpUserMap)
					continue
				}

				var (
					tmpQty        float64
					quantity      string
					quantityFloat float64
					side          string
					positionSide  string
					orderType     = "MARKET"
				)
				if "LONG" == tmpInsertData.PositionSide {
					positionSide = "LONG"
					side = "BUY"
				} else if "SHORT" == tmpInsertData.PositionSide {
					positionSide = "SHORT"
					side = "SELL"
				} else if "BOTH" == tmpInsertData.PositionSide {
					if math.Signbit(tmpInsertData.PositionAmount) {
						positionSide = "SHORT"
						side = "SELL"
					} else {
						positionSide = "LONG"
						side = "BUY"
					}
				} else {
					fmt.Println("龟兔，新增用户，无效信息，信息", vInsertData)
					continue
				}
				tmpPositionAmount := math.Abs(tmpInsertData.PositionAmount)
				// 本次 代单员币的数量 * (用户保证金/代单员保证金)
				tmpQty = tmpPositionAmount * tmpUserBindTradersAmount / tmpTraderBaseMoney // 本次开单数量

				// 精度调整
				if 0 >= symbolsMap.Get(tmpInsertData.Symbol).(*entity.LhCoinSymbol).QuantityPrecision {
					quantity = fmt.Sprintf("%d", int64(tmpQty))
				} else {
					quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap.Get(tmpInsertData.Symbol).(*entity.LhCoinSymbol).QuantityPrecision, 64)
				}

				quantityFloat, err = strconv.ParseFloat(quantity, 64)
				if nil != err {
					fmt.Println(err)
					continue
				}

				if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
					continue
				}

				// 下单，不用计算数量，新仓位
				// 新订单数据
				currentOrder := &entity.NewUserOrderTwo{
					UserId:        vTmpUserMap.Id,
					TraderId:      1,
					Symbol:        tmpInsertData.Symbol,
					Side:          side,
					PositionSide:  positionSide,
					Quantity:      quantityFloat,
					Price:         0,
					TraderQty:     tmpPositionAmount,
					OrderType:     orderType,
					ClosePosition: "",
					CumQuote:      0,
					ExecutedQty:   0,
					AvgPrice:      0,
				}

				var (
					binanceOrderRes *binanceOrder
					orderInfoRes    *orderInfo
				)
				// 请求下单
				binanceOrderRes, orderInfoRes, err = requestBinanceOrder(tmpInsertData.Symbol, side, orderType, positionSide, quantity, vTmpUserMap.ApiKey, vTmpUserMap.ApiSecret)
				if nil != err {
					fmt.Println(err)
				}

				//binanceOrderRes = &binanceOrder{
				//	OrderId:       1,
				//	ExecutedQty:   quantity,
				//	ClientOrderId: "",
				//	Symbol:        "",
				//	AvgPrice:      "",
				//	CumQuote:      "",
				//	Side:          "",
				//	PositionSide:  "",
				//	ClosePosition: false,
				//	Type:          "",
				//	Status:        "",
				//}

				// 下单异常
				if 0 >= binanceOrderRes.OrderId {
					orderErr.Add(&entity.NewUserOrderErrTwo{
						UserId:        currentOrder.UserId,
						TraderId:      currentOrder.TraderId,
						ClientOrderId: "",
						OrderId:       "",
						Symbol:        currentOrder.Symbol,
						Side:          currentOrder.Side,
						PositionSide:  currentOrder.PositionSide,
						Quantity:      quantityFloat,
						Price:         currentOrder.Price,
						TraderQty:     currentOrder.TraderQty,
						OrderType:     currentOrder.OrderType,
						ClosePosition: currentOrder.ClosePosition,
						CumQuote:      currentOrder.CumQuote,
						ExecutedQty:   currentOrder.ExecutedQty,
						AvgPrice:      currentOrder.AvgPrice,
						HandleStatus:  currentOrder.HandleStatus,
						Code:          int(orderInfoRes.Code),
						Msg:           orderInfoRes.Msg,
						Proportion:    0,
					})

					fmt.Println(orderInfoRes)
					continue
				}

				var tmpExecutedQty float64
				tmpExecutedQty, err = strconv.ParseFloat(binanceOrderRes.ExecutedQty, 64)
				if nil != err {
					fmt.Println("龟兔，新增用户，下单错误，解析错误2，信息", err, currentOrder, binanceOrderRes)
					continue
				}

				// 不存在新增，这里只能是开仓
				if !orderMap.Contains(tmpInsertData.Symbol + "&" + positionSide + "&" + strUserId) {
					orderMap.Set(tmpInsertData.Symbol+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
				} else {
					tmpExecutedQty += orderMap.Get(tmpInsertData.Symbol + "&" + positionSide + "&" + strUserId).(float64)
					orderMap.Set(tmpInsertData.Symbol+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
				}
			}

		}

		globalUsers.Set(k, vTmpUserMap)
	}

	// 第二遍比较，删除
	tmpIds := make([]uint, 0)
	globalUsers.Iterator(func(k interface{}, v interface{}) bool {
		if _, ok := tmpUserMap[k.(uint)]; !ok {
			tmpIds = append(tmpIds, k.(uint))
		}
		return true
	})

	// 删除的人
	for _, vTmpIds := range tmpIds {
		fmt.Println("龟兔，删除用户:", vTmpIds)
		globalUsers.Remove(vTmpIds)

		tmpRemoveUserKey := make([]string, 0)
		// 遍历map
		orderMap.Iterator(func(k interface{}, v interface{}) bool {
			parts := strings.Split(k.(string), "&")
			if 3 != len(parts) {
				return true
			}

			var (
				uid uint64
			)
			uid, err = strconv.ParseUint(parts[2], 10, 64)
			if nil != err {
				fmt.Println("龟兔，删除用户,解析id错误:", vTmpIds)
			}

			if uid != uint64(vTmpIds) {
				return true
			}

			tmpRemoveUserKey = append(tmpRemoveUserKey, k.(string))
			return true
		})

		for _, vK := range tmpRemoveUserKey {
			if orderMap.Contains(vK) {
				orderMap.Remove(vK)
			}
		}
	}
}

// PullAndOrderNewGuiTu 拉取binance数据，仓位，根据cookie 龟兔赛跑
func (s *sBinanceTraderHistory) PullAndOrderNewGuiTu(ctx context.Context) {
	var (
		traderNum                 = globalTraderNum // 龟兔
		zyTraderCookie            []*entity.ZyTraderCookie
		binancePositionMapCompare map[string]*entity.TraderPosition
		reqResData                []*binancePositionDataList
		cookie                    = "no"
		token                     = "no"
		err                       error
	)

	// 执行
	for {
		//time.Sleep(5 * time.Second)
		time.Sleep(28 * time.Millisecond)
		start := time.Now()

		// 重新初始化数据
		if 0 < len(binancePositionMap) {
			binancePositionMapCompare = make(map[string]*entity.TraderPosition, 0)
			for k, vBinancePositionMap := range binancePositionMap {
				binancePositionMapCompare[k] = vBinancePositionMap
			}
		}

		if "no" == cookie || "no" == token {
			// 数据库必须信息
			err = g.Model("zy_trader_cookie").Ctx(ctx).Where("trader_id=? and is_open=?", 1, 1).
				OrderDesc("update_time").Limit(1).Scan(&zyTraderCookie)
			if nil != err {
				//fmt.Println("龟兔，cookie，数据库查询错误：", err)
				time.Sleep(time.Second * 3)
				continue
			}

			if 0 >= len(zyTraderCookie) || 0 >= len(zyTraderCookie[0].Cookie) || 0 >= len(zyTraderCookie[0].Token) {
				//fmt.Println("龟兔，cookie，无可用：", err)
				time.Sleep(time.Second * 3)
				continue
			}

			// 更新
			cookie = zyTraderCookie[0].Cookie
			token = zyTraderCookie[0].Token
		}

		// 执行
		var (
			retry           = false
			retryTimes      = 0
			retryTimesLimit = 5 // 重试次数
			cookieErr       = false
		)

		for retryTimes < retryTimesLimit { // 最大重试
			// 龟兔的数据
			reqResData, retry, err = s.requestBinancePositionHistoryNew(traderNum, cookie, token)
			//reqResData, retry, err = s.requestProxyBinancePositionHistoryNew("http://43.130.227.135:888/", traderNum, cookie, token)

			// 需要重试
			if retry {
				retryTimes++
				time.Sleep(time.Second * 5)
				fmt.Println("龟兔，重试：", retry)
				continue
			}

			// cookie不好使
			if 0 >= len(reqResData) {
				retryTimes++
				cookieErr = true
				continue
			} else {
				cookieErr = false
				break
			}
		}

		// 记录时间
		timePull := time.Since(start)

		// cookie 错误
		if cookieErr {
			cookie = "no"
			token = "no"

			fmt.Println("龟兔，cookie错误，信息", traderNum, reqResData)
			err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
				zyTraderCookie[0].IsOpen = 0
				_, err = tx.Ctx(ctx).Update("zy_trader_cookie", zyTraderCookie[0], "id", zyTraderCookie[0].Id)
				if nil != err {
					fmt.Println("龟兔，cookie错误，信息", traderNum, reqResData)
					return err
				}

				return nil
			})
			if nil != err {
				fmt.Println("龟兔，cookie错误，更新数据库错误，信息", traderNum, err)
			}

			continue
		}

		// 用于数据库更新
		insertData := make([]*do.TraderPosition, 0)
		updateData := make([]*do.TraderPosition, 0)
		// 用于下单
		orderInsertData := make([]*do.TraderPosition, 0)
		orderUpdateData := make([]*do.TraderPosition, 0)
		for _, vReqResData := range reqResData {
			// 新增
			var (
				currentAmount    float64
				currentAmountAbs float64
			)
			currentAmount, err = strconv.ParseFloat(vReqResData.PositionAmount, 64)
			if nil != err {
				fmt.Println("新，解析金额出错，信息", vReqResData, currentAmount, traderNum)
			}
			currentAmountAbs = math.Abs(currentAmount) // 绝对值

			if _, ok := binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide]; !ok {
				if "BOTH" != vReqResData.PositionSide { // 单项持仓
					// 加入数据库
					insertData = append(insertData, &do.TraderPosition{
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmountAbs,
					})

					// 下单
					if IsEqual(currentAmountAbs, 0) {
						continue
					}

					orderInsertData = append(orderInsertData, &do.TraderPosition{
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmountAbs,
					})
				} else {
					// 加入数据库
					insertData = append(insertData, &do.TraderPosition{
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmount, // 正负数保持
					})

					// 模拟为多空仓，下单，todo 组合式的判断应该时牢靠的
					var tmpPositionSide string
					if IsEqual(currentAmount, 0) {
						continue
					} else if math.Signbit(currentAmount) {
						// 模拟空
						tmpPositionSide = "SHORT"
						orderInsertData = append(orderInsertData, &do.TraderPosition{
							Symbol:         vReqResData.Symbol,
							PositionSide:   tmpPositionSide,
							PositionAmount: currentAmountAbs, // 变成绝对值
						})
					} else {
						// 模拟多
						tmpPositionSide = "LONG"
						orderInsertData = append(orderInsertData, &do.TraderPosition{
							Symbol:         vReqResData.Symbol,
							PositionSide:   tmpPositionSide,
							PositionAmount: currentAmountAbs, // 变成绝对值
						})
					}
				}
			} else {
				// 数量无变化
				if "BOTH" != vReqResData.PositionSide {
					if IsEqual(currentAmountAbs, binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount) {
						continue
					}

					updateData = append(updateData, &do.TraderPosition{
						Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmountAbs,
					})

					orderUpdateData = append(orderUpdateData, &do.TraderPosition{
						Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmountAbs,
					})
				} else {
					if IsEqual(currentAmount, binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount) {
						continue
					}

					updateData = append(updateData, &do.TraderPosition{
						Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
						Symbol:         vReqResData.Symbol,
						PositionSide:   vReqResData.PositionSide,
						PositionAmount: currentAmount, // 正负数保持
					})

					// 第一步：构造虚拟的上一次仓位，空或多或无
					// 这里修改一下历史仓位的信息，方便程序在后续的流程中使用，模拟both的positionAmount为正数时，修改仓位对应的多仓方向的数据，为负数时修改空仓位的数据，0时不处理
					if _, ok = binancePositionMap[vReqResData.Symbol+"SHORT"]; !ok {
						fmt.Println("新，缺少仓位SHORT，信息", binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide])
						continue
					}
					if _, ok = binancePositionMap[vReqResData.Symbol+"LONG"]; !ok {
						fmt.Println("新，缺少仓位LONG，信息", binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide])
						continue
					}

					var lastPositionSide string // 上次仓位
					binancePositionMapCompare[vReqResData.Symbol+"SHORT"] = &entity.TraderPosition{
						Id:             binancePositionMapCompare[vReqResData.Symbol+"SHORT"].Id,
						Symbol:         binancePositionMapCompare[vReqResData.Symbol+"SHORT"].Symbol,
						PositionSide:   binancePositionMapCompare[vReqResData.Symbol+"SHORT"].PositionSide,
						PositionAmount: 0,
						CreatedAt:      binancePositionMapCompare[vReqResData.Symbol+"SHORT"].CreatedAt,
						UpdatedAt:      binancePositionMapCompare[vReqResData.Symbol+"SHORT"].UpdatedAt,
					}
					binancePositionMapCompare[vReqResData.Symbol+"LONG"] = &entity.TraderPosition{
						Id:             binancePositionMapCompare[vReqResData.Symbol+"LONG"].Id,
						Symbol:         binancePositionMapCompare[vReqResData.Symbol+"LONG"].Symbol,
						PositionSide:   binancePositionMapCompare[vReqResData.Symbol+"LONG"].PositionSide,
						PositionAmount: 0,
						CreatedAt:      binancePositionMapCompare[vReqResData.Symbol+"LONG"].CreatedAt,
						UpdatedAt:      binancePositionMapCompare[vReqResData.Symbol+"LONG"].UpdatedAt,
					}

					if IsEqual(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount, 0) { // both仓为0
						// 认为两仓都无

					} else if math.Signbit(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount) {
						lastPositionSide = "SHORT"
						binancePositionMapCompare[vReqResData.Symbol+"SHORT"].PositionAmount = math.Abs(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount)
					} else {
						lastPositionSide = "LONG"
						binancePositionMapCompare[vReqResData.Symbol+"LONG"].PositionAmount = math.Abs(binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].PositionAmount)
					}

					// 本次仓位
					var tmpPositionSide string
					if IsEqual(currentAmount, 0) { // 本次仓位是0
						if 0 >= len(lastPositionSide) {
							// 本次和上一次仓位都是0，应该不会走到这里
							fmt.Println("新，仓位异常逻辑，信息", binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide])
							continue
						}

						// 仍为是一次完全平仓，仓位和上一次保持一致
						tmpPositionSide = lastPositionSide
					} else if math.Signbit(currentAmount) { // 判断有无符号
						// 第二步：本次仓位

						// 上次和本次相反需要平上次
						if "LONG" == lastPositionSide {
							orderUpdateData = append(orderUpdateData, &do.TraderPosition{
								Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
								Symbol:         vReqResData.Symbol,
								PositionSide:   lastPositionSide,
								PositionAmount: float64(0),
							})
						}

						tmpPositionSide = "SHORT"
					} else {
						// 第二步：本次仓位

						// 上次和本次相反需要平上次
						if "SHORT" == lastPositionSide {
							orderUpdateData = append(orderUpdateData, &do.TraderPosition{
								Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
								Symbol:         vReqResData.Symbol,
								PositionSide:   lastPositionSide,
								PositionAmount: float64(0),
							})
						}

						tmpPositionSide = "LONG"
					}

					orderUpdateData = append(orderUpdateData, &do.TraderPosition{
						Id:             binancePositionMap[vReqResData.Symbol+vReqResData.PositionSide].Id,
						Symbol:         vReqResData.Symbol,
						PositionSide:   tmpPositionSide,
						PositionAmount: currentAmountAbs,
					})
				}
			}
		}

		if 0 >= len(insertData) && 0 >= len(updateData) {
			continue
		}

		// 新增数据
		tmpIdCurrent := len(binancePositionMap) + 1
		for _, vIBinancePosition := range insertData {
			binancePositionMap[vIBinancePosition.Symbol.(string)+vIBinancePosition.PositionSide.(string)] = &entity.TraderPosition{
				Id:             uint(tmpIdCurrent),
				Symbol:         vIBinancePosition.Symbol.(string),
				PositionSide:   vIBinancePosition.PositionSide.(string),
				PositionAmount: vIBinancePosition.PositionAmount.(float64),
			}
		}

		// 更新仓位数据
		for _, vUBinancePosition := range updateData {
			binancePositionMap[vUBinancePosition.Symbol.(string)+vUBinancePosition.PositionSide.(string)] = &entity.TraderPosition{
				Id:             vUBinancePosition.Id.(uint),
				Symbol:         vUBinancePosition.Symbol.(string),
				PositionSide:   vUBinancePosition.PositionSide.(string),
				PositionAmount: vUBinancePosition.PositionAmount.(float64),
			}
		}

		// 推送订单，数据库已初始化仓位，新仓库
		if 0 >= len(binancePositionMapCompare) {
			fmt.Println("初始化仓位成功")
			continue
		}

		fmt.Printf("龟兔，程序拉取部分，开始 %v, 拉取时长: %v, 统计更新时长: %v\n", start, timePull, time.Since(start))

		wg := sync.WaitGroup{}
		// 遍历跟单者
		tmpTraderBaseMoney := baseMoneyGuiTu.Val()
		globalUsers.Iterator(func(k interface{}, v interface{}) bool {
			tmpUser := v.(*entity.NewUser)

			var tmpUserBindTradersAmount float64
			if !baseMoneyUserAllMap.Contains(int(tmpUser.Id)) {
				fmt.Println("龟兔，保证金不存在：", tmpUser)
				return true
			}
			tmpUserBindTradersAmount = baseMoneyUserAllMap.Get(int(tmpUser.Id)).(float64)

			if lessThanOrEqualZero(tmpUserBindTradersAmount, 0, 1e-7) {
				fmt.Println("龟兔，保证金不足为0：", tmpUserBindTradersAmount, tmpUser)
				return true
			}

			strUserId := strconv.FormatUint(uint64(tmpUser.Id), 10)
			if 0 >= len(tmpUser.ApiSecret) || 0 >= len(tmpUser.ApiKey) {
				fmt.Println("龟兔，用户的信息无效了，信息", traderNum, tmpUser)
				return true
			}

			if lessThanOrEqualZero(tmpTraderBaseMoney, 0, 1e-7) {
				fmt.Println("龟兔，交易员信息无效了，信息", tmpUser)
				return true
			}

			// 新增仓位
			for _, vInsertData := range orderInsertData {
				// 一个新symbol通常3个开仓方向short，long，both，屏蔽一下未真实开仓的
				tmpInsertData := vInsertData
				if lessThanOrEqualZero(tmpInsertData.PositionAmount.(float64), 0, 1e-7) {
					continue
				}

				if !symbolsMap.Contains(tmpInsertData.Symbol.(string)) {
					fmt.Println("龟兔，代币信息无效，信息", tmpInsertData, tmpUser)
					continue
				}

				var (
					tmpQty        float64
					quantity      string
					quantityFloat float64
					side          string
					positionSide  string
					orderType     = "MARKET"
				)
				if "LONG" == tmpInsertData.PositionSide {
					positionSide = "LONG"
					side = "BUY"
				} else if "SHORT" == tmpInsertData.PositionSide {
					positionSide = "SHORT"
					side = "SELL"
				} else {
					fmt.Println("龟兔，无效信息，信息", vInsertData)
					continue
				}

				// 本次 代单员币的数量 * (用户保证金/代单员保证金)
				tmpQty = tmpInsertData.PositionAmount.(float64) * tmpUserBindTradersAmount / tmpTraderBaseMoney // 本次开单数量

				// 精度调整
				if 0 >= symbolsMap.Get(tmpInsertData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision {
					quantity = fmt.Sprintf("%d", int64(tmpQty))
				} else {
					quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap.Get(tmpInsertData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision, 64)
				}

				quantityFloat, err = strconv.ParseFloat(quantity, 64)
				if nil != err {
					fmt.Println(err)
					continue
				}

				if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
					continue
				}

				wg.Add(1)
				err = s.pool.Add(ctx, func(ctx context.Context) {
					defer wg.Done()

					// 下单，不用计算数量，新仓位
					// 新订单数据
					currentOrder := &entity.NewUserOrderTwo{
						UserId:        tmpUser.Id,
						TraderId:      1,
						Symbol:        tmpInsertData.Symbol.(string),
						Side:          side,
						PositionSide:  positionSide,
						Quantity:      quantityFloat,
						Price:         0,
						TraderQty:     tmpInsertData.PositionAmount.(float64),
						OrderType:     orderType,
						ClosePosition: "",
						CumQuote:      0,
						ExecutedQty:   0,
						AvgPrice:      0,
					}

					var (
						binanceOrderRes *binanceOrder
						orderInfoRes    *orderInfo
					)
					// 请求下单
					binanceOrderRes, orderInfoRes, err = requestBinanceOrder(tmpInsertData.Symbol.(string), side, orderType, positionSide, quantity, tmpUser.ApiKey, tmpUser.ApiSecret)
					if nil != err {
						fmt.Println(err)
					}

					//binanceOrderRes = &binanceOrder{
					//	OrderId:       1,
					//	ExecutedQty:   quantity,
					//	ClientOrderId: "",
					//	Symbol:        "",
					//	AvgPrice:      "",
					//	CumQuote:      "",
					//	Side:          "",
					//	PositionSide:  "",
					//	ClosePosition: false,
					//	Type:          "",
					//	Status:        "",
					//}

					// 下单异常
					if 0 >= binanceOrderRes.OrderId {
						orderErr.Add(&entity.NewUserOrderErrTwo{
							UserId:        currentOrder.UserId,
							TraderId:      currentOrder.TraderId,
							ClientOrderId: "",
							OrderId:       "",
							Symbol:        currentOrder.Symbol,
							Side:          currentOrder.Side,
							PositionSide:  currentOrder.PositionSide,
							Quantity:      quantityFloat,
							Price:         currentOrder.Price,
							TraderQty:     currentOrder.TraderQty,
							OrderType:     currentOrder.OrderType,
							ClosePosition: currentOrder.ClosePosition,
							CumQuote:      currentOrder.CumQuote,
							ExecutedQty:   currentOrder.ExecutedQty,
							AvgPrice:      currentOrder.AvgPrice,
							HandleStatus:  currentOrder.HandleStatus,
							Code:          int(orderInfoRes.Code),
							Msg:           orderInfoRes.Msg,
							Proportion:    0,
						})

						fmt.Println(orderInfoRes)
						return
					}

					var tmpExecutedQty float64
					tmpExecutedQty, err = strconv.ParseFloat(binanceOrderRes.ExecutedQty, 64)
					if nil != err {
						fmt.Println("龟兔，下单错误，解析错误2，信息", err, currentOrder, binanceOrderRes)
						return
					}

					// 不存在新增，这里只能是开仓
					if !orderMap.Contains(tmpInsertData.Symbol.(string) + "&" + positionSide + "&" + strUserId) {
						orderMap.Set(tmpInsertData.Symbol.(string)+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
					} else {
						tmpExecutedQty += orderMap.Get(tmpInsertData.Symbol.(string) + "&" + positionSide + "&" + strUserId).(float64)
						orderMap.Set(tmpInsertData.Symbol.(string)+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
					}

					return
				})
				if nil != err {
					fmt.Println("龟兔，添加下单任务异常，新增仓位，错误信息：", err, traderNum, vInsertData, tmpUser)
				}
			}

			// 修改仓位
			for _, vUpdateData := range orderUpdateData {
				tmpUpdateData := vUpdateData
				if _, ok := binancePositionMapCompare[vUpdateData.Symbol.(string)+vUpdateData.PositionSide.(string)]; !ok {
					fmt.Println("龟兔，添加下单任务异常，修改仓位，错误信息：", err, traderNum, vUpdateData, tmpUser)
					continue
				}
				lastPositionData := binancePositionMapCompare[vUpdateData.Symbol.(string)+vUpdateData.PositionSide.(string)]

				if !symbolsMap.Contains(tmpUpdateData.Symbol.(string)) {
					fmt.Println("龟兔，代币信息无效，信息", tmpUpdateData, tmpUser)
					continue
				}

				var (
					tmpQty        float64
					quantity      string
					quantityFloat float64
					side          string
					positionSide  string
					orderType     = "MARKET"
				)

				if lessThanOrEqualZero(tmpUpdateData.PositionAmount.(float64), 0, 1e-7) {
					fmt.Println("龟兔，完全平仓：", tmpUpdateData)
					// 全平仓
					if "LONG" == tmpUpdateData.PositionSide {
						positionSide = "LONG"
						side = "SELL"
					} else if "SHORT" == tmpUpdateData.PositionSide {
						positionSide = "SHORT"
						side = "BUY"
					} else {
						fmt.Println("龟兔，无效信息，信息", tmpUpdateData)
						continue
					}

					// 未开启过仓位
					if !orderMap.Contains(tmpUpdateData.Symbol.(string) + "&" + tmpUpdateData.PositionSide.(string) + "&" + strUserId) {
						continue
					}

					// 认为是0
					if lessThanOrEqualZero(orderMap.Get(tmpUpdateData.Symbol.(string)+"&"+tmpUpdateData.PositionSide.(string)+"&"+strUserId).(float64), 0, 1e-7) {
						continue
					}

					// 剩余仓位
					tmpQty = orderMap.Get(tmpUpdateData.Symbol.(string) + "&" + tmpUpdateData.PositionSide.(string) + "&" + strUserId).(float64)
				} else if lessThanOrEqualZero(lastPositionData.PositionAmount, tmpUpdateData.PositionAmount.(float64), 1e-7) {
					fmt.Println("龟兔，追加仓位：", tmpUpdateData, lastPositionData)
					// 本次加仓 代单员币的数量 * (用户保证金/代单员保证金)
					if "LONG" == tmpUpdateData.PositionSide {
						positionSide = "LONG"
						side = "BUY"
					} else if "SHORT" == tmpUpdateData.PositionSide {
						positionSide = "SHORT"
						side = "SELL"
					} else {
						fmt.Println("龟兔，无效信息，信息", tmpUpdateData)
						continue
					}

					// 本次减去上一次
					tmpQty = (tmpUpdateData.PositionAmount.(float64) - lastPositionData.PositionAmount) * tmpUserBindTradersAmount / tmpTraderBaseMoney // 本次开单数量
				} else if lessThanOrEqualZero(tmpUpdateData.PositionAmount.(float64), lastPositionData.PositionAmount, 1e-7) {
					fmt.Println("龟兔，部分平仓：", tmpUpdateData, lastPositionData)
					// 部分平仓
					if "LONG" == tmpUpdateData.PositionSide {
						positionSide = "LONG"
						side = "SELL"
					} else if "SHORT" == tmpUpdateData.PositionSide {
						positionSide = "SHORT"
						side = "BUY"
					} else {
						fmt.Println("龟兔，无效信息，信息", tmpUpdateData)
						continue
					}

					// 未开启过仓位
					if !orderMap.Contains(tmpUpdateData.Symbol.(string) + "&" + tmpUpdateData.PositionSide.(string) + "&" + strUserId) {
						continue
					}

					// 认为是0
					if lessThanOrEqualZero(orderMap.Get(tmpUpdateData.Symbol.(string)+"&"+tmpUpdateData.PositionSide.(string)+"&"+strUserId).(float64), 0, 1e-7) {
						continue
					}

					// 上次仓位
					if lessThanOrEqualZero(lastPositionData.PositionAmount, 0, 1e-7) {
						fmt.Println("龟兔，部分平仓，上次仓位信息无效，信息", lastPositionData, tmpUpdateData)
						continue
					}

					// 按百分比
					tmpQty = orderMap.Get(tmpUpdateData.Symbol.(string)+"&"+tmpUpdateData.PositionSide.(string)+"&"+strUserId).(float64) * (lastPositionData.PositionAmount - tmpUpdateData.PositionAmount.(float64)) / lastPositionData.PositionAmount
				} else {
					fmt.Println("龟兔，分析仓位无效，信息", lastPositionData, tmpUpdateData)
					continue
				}

				// 精度调整
				if 0 >= symbolsMap.Get(tmpUpdateData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision {
					quantity = fmt.Sprintf("%d", int64(tmpQty))
				} else {
					quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap.Get(tmpUpdateData.Symbol.(string)).(*entity.LhCoinSymbol).QuantityPrecision, 64)
				}

				quantityFloat, err = strconv.ParseFloat(quantity, 64)
				if nil != err {
					fmt.Println(err)
					continue
				}

				if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
					continue
				}

				wg.Add(1)
				err = s.pool.Add(ctx, func(ctx context.Context) {
					defer wg.Done()

					// 下单，不用计算数量，新仓位
					// 新订单数据
					currentOrder := &entity.NewUserOrderTwo{
						UserId:        tmpUser.Id,
						TraderId:      1,
						Symbol:        tmpUpdateData.Symbol.(string),
						Side:          side,
						PositionSide:  positionSide,
						Quantity:      quantityFloat,
						Price:         0,
						TraderQty:     quantityFloat,
						OrderType:     orderType,
						ClosePosition: "",
						CumQuote:      0,
						ExecutedQty:   0,
						AvgPrice:      0,
					}

					var (
						binanceOrderRes *binanceOrder
						orderInfoRes    *orderInfo
					)
					// 请求下单
					binanceOrderRes, orderInfoRes, err = requestBinanceOrder(tmpUpdateData.Symbol.(string), side, orderType, positionSide, quantity, tmpUser.ApiKey, tmpUser.ApiSecret)
					if nil != err {
						fmt.Println(err)
						return
					}

					//binanceOrderRes = &binanceOrder{
					//	OrderId:       1,
					//	ExecutedQty:   quantity,
					//	ClientOrderId: "",
					//	Symbol:        "",
					//	AvgPrice:      "",
					//	CumQuote:      "",
					//	Side:          "",
					//	PositionSide:  "",
					//	ClosePosition: false,
					//	Type:          "",
					//	Status:        "",
					//}

					// 下单异常
					if 0 >= binanceOrderRes.OrderId {
						// 写入
						orderErr.Add(&entity.NewUserOrderErrTwo{
							UserId:        currentOrder.UserId,
							TraderId:      currentOrder.TraderId,
							ClientOrderId: "",
							OrderId:       "",
							Symbol:        currentOrder.Symbol,
							Side:          currentOrder.Side,
							PositionSide:  currentOrder.PositionSide,
							Quantity:      quantityFloat,
							Price:         currentOrder.Price,
							TraderQty:     currentOrder.TraderQty,
							OrderType:     currentOrder.OrderType,
							ClosePosition: currentOrder.ClosePosition,
							CumQuote:      currentOrder.CumQuote,
							ExecutedQty:   currentOrder.ExecutedQty,
							AvgPrice:      currentOrder.AvgPrice,
							HandleStatus:  currentOrder.HandleStatus,
							Code:          int(orderInfoRes.Code),
							Msg:           orderInfoRes.Msg,
							Proportion:    0,
						})

						fmt.Println(orderInfoRes)
						return // 返回
					}

					var tmpExecutedQty float64
					tmpExecutedQty, err = strconv.ParseFloat(binanceOrderRes.ExecutedQty, 64)
					if nil != err {
						fmt.Println("龟兔，下单错误，解析错误2，信息", err, currentOrder, binanceOrderRes)
						return
					}

					// 不存在新增，这里只能是开仓
					if !orderMap.Contains(tmpUpdateData.Symbol.(string) + "&" + positionSide + "&" + strUserId) {
						// 追加仓位，开仓
						if "LONG" == positionSide && "BUY" == side {
							orderMap.Set(tmpUpdateData.Symbol.(string)+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
						} else if "SHORT" == positionSide && "SELL" == side {
							orderMap.Set(tmpUpdateData.Symbol.(string)+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
						} else {
							fmt.Println("龟兔，未知仓位信息，信息", tmpUpdateData, tmpExecutedQty)
						}

					} else {
						// 追加仓位，开仓
						if "LONG" == positionSide {
							if "BUY" == side {
								tmpExecutedQty += orderMap.Get(tmpUpdateData.Symbol.(string) + "&" + positionSide + "&" + strUserId).(float64)
								orderMap.Set(tmpUpdateData.Symbol.(string)+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
							} else if "SELL" == side {
								tmpExecutedQty = orderMap.Get(tmpUpdateData.Symbol.(string)+"&"+positionSide+"&"+strUserId).(float64) - tmpExecutedQty
								if lessThanOrEqualZero(tmpExecutedQty, 0, 1e-7) {
									tmpExecutedQty = 0
								}
								orderMap.Set(tmpUpdateData.Symbol.(string)+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
							} else {
								fmt.Println("龟兔，未知仓位信息，信息", tmpUpdateData, tmpExecutedQty)
							}

						} else if "SHORT" == positionSide {
							if "SELL" == side {
								tmpExecutedQty += orderMap.Get(tmpUpdateData.Symbol.(string) + "&" + positionSide + "&" + strUserId).(float64)
								orderMap.Set(tmpUpdateData.Symbol.(string)+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
							} else if "BUY" == side {
								tmpExecutedQty = orderMap.Get(tmpUpdateData.Symbol.(string)+"&"+positionSide+"&"+strUserId).(float64) - tmpExecutedQty
								if lessThanOrEqualZero(tmpExecutedQty, 0, 1e-7) {
									tmpExecutedQty = 0
								}
								orderMap.Set(tmpUpdateData.Symbol.(string)+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
							} else {
								fmt.Println("龟兔，未知仓位信息，信息", tmpUpdateData, tmpExecutedQty)
							}

						} else {
							fmt.Println("龟兔，未知仓位信息，信息", tmpUpdateData, tmpExecutedQty)
						}
					}

					return
				})
				if nil != err {
					fmt.Println("新，添加下单任务异常，修改仓位，错误信息：", err, traderNum, vUpdateData, tmpUser)
				}
			}

			return true
		})

		// 回收协程
		wg.Wait()

		fmt.Printf("龟兔，程序执行完毕，开始 %v, 拉取时长: %v, 总计时长: %v\n", start, timePull, time.Since(start))
	}
}

// pullAndSetHandle 拉取的binance数据细节
func (s *sBinanceTraderHistory) pullAndSetHandle(ctx context.Context, traderNum uint64, CountPage int, ipOrderAsc bool, ipMapNeedWait map[string]bool) (resData []*entity.NewBinanceTradeHistory, err error) {
	var (
		PerPullPerPageCountLimitMax = 50 // 每次并行拉取每页最大条数
	)

	if 0 >= s.ips.Size() {
		fmt.Println("ip池子不足，目前数量：", s.ips.Size())
		return nil, err
	}

	// 定义协程共享数据
	dataMap := gmap.New(true) // 结果map，key表示页数，并发安全
	defer dataMap.Clear()
	ipsQueue := gqueue.New() // ip通道，首次使用只用一次
	defer ipsQueue.Close()
	ipsQueueNeedWait := gqueue.New() // ip通道需要等待的
	defer ipsQueueNeedWait.Close()

	// 这里注意一定是key是从0开始到size-1的
	if ipOrderAsc {
		for i := 0; i < s.ips.Size(); i++ {
			if _, ok := ipMapNeedWait[s.ips.Get(i)]; ok {
				if 0 < len(s.ips.Get(i)) { // 1页对应的代理ip的map的key是0
					ipsQueueNeedWait.Push(s.ips.Get(i))
					//if 3949214983441029120 == traderNum {
					//	fmt.Println("暂时别用的ip", s.ips.Get(i))
					//}
				}
			} else {
				if 0 < len(s.ips.Get(i)) { // 1页对应的代理ip的map的key是0
					ipsQueue.Push(s.ips.Get(i))
				}
			}
		}
	} else {
		for i := s.ips.Size() - 1; i >= 0; i-- {
			if _, ok := ipMapNeedWait[s.ips.Get(i)]; ok {
				if 0 < len(s.ips.Get(i)) { // 1页对应的代理ip的map的key是0
					ipsQueueNeedWait.Push(s.ips.Get(i))
					//if 3949214983441029120 == traderNum {
					//	fmt.Println("暂时别用的ip", s.ips.Get(i))
					//}
				}
			} else {
				if 0 < len(s.ips.Get(i)) { // 1页对应的代理ip的map的key是0
					ipsQueue.Push(s.ips.Get(i))
				}
			}
		}
	}

	wg := sync.WaitGroup{}
	for i := 1; i <= CountPage; i++ {
		tmpI := i // go1.22以前的循环陷阱

		wg.Add(1)
		err = s.pool.Add(ctx, func(ctx context.Context) {
			defer wg.Done()

			var (
				retry               = false
				retryTimes          = 0
				retryTimesLimit     = 5 // 重试次数
				successPull         bool
				binanceTradeHistory []*binanceTradeHistoryDataList
			)

			for retryTimes < retryTimesLimit { // 最大重试
				var tmpProxy string
				if 0 < ipsQueue.Len() { // 有剩余先用剩余比较快，不用等2s
					if v := ipsQueue.Pop(); nil != v {
						tmpProxy = v.(string)
						//if 3949214983441029120 == traderNum {
						//	fmt.Println("直接开始", tmpProxy)
						//}
					}
				}

				// 如果没拿到剩余池子
				if 0 >= len(tmpProxy) {
					select {
					case queueItem2 := <-ipsQueueNeedWait.C: // 可用ip，阻塞
						tmpProxy = queueItem2.(string)
						//if 3949214983441029120 == traderNum {
						//	fmt.Println("等待", tmpProxy)
						//}
						time.Sleep(time.Second * 2)
					case <-time.After(time.Minute * 8): // 即使1个ip轮流用，120次查询2秒一次，8分钟超时足够
						fmt.Println("timeout, exit loop")
						break
					}
				}

				// 拿到了代理，执行
				if 0 < len(tmpProxy) {
					binanceTradeHistory, retry, err = s.requestProxyBinanceTradeHistory(tmpProxy, int64(tmpI), int64(PerPullPerPageCountLimitMax), traderNum)
					if nil != err {
						//fmt.Println(err)
					}

					// 使用过的，释放，推入等待queue
					ipsQueueNeedWait.Push(tmpProxy)
				}

				// 需要重试
				if retry {
					//if 3949214983441029120 == traderNum {
					//	fmt.Println("异常需要重试", tmpProxy)
					//}

					retryTimes++
					continue
				}

				// 设置数据
				successPull = true
				dataMap.Set(tmpI, binanceTradeHistory)
				break // 成功直接结束
			}

			// 如果重试次数超过限制且没有成功，存入标记值
			if !successPull {
				dataMap.Set(tmpI, "ERROR")
			}
		})

		if nil != err {
			fmt.Println("添加任务，拉取数据协程异常，错误信息：", err, "交易员：", traderNum)
		}
	}

	// 回收协程
	wg.Wait()

	// 结果，解析处理
	resData = make([]*entity.NewBinanceTradeHistory, 0)
	for i := 1; i <= CountPage; i++ {
		if dataMap.Contains(i) {
			// 从dataMap中获取该页的数据
			dataInterface := dataMap.Get(i)

			// 检查是否是标记值
			if "ERROR" == dataInterface {
				fmt.Println("数据拉取失败，页数：", i, "交易员：", traderNum)
				return nil, err
			}

			// 类型断言，确保dataInterface是我们期望的类型
			if data, ok := dataInterface.([]*binanceTradeHistoryDataList); ok {
				// 现在data是一个binanceTradeHistoryDataList对象数组
				for _, item := range data {
					// 类型处理
					tmpActiveBuy := "false"
					if item.ActiveBuy {
						tmpActiveBuy = "true"
					}

					if "LONG" != item.PositionSide && "SHORT" != item.PositionSide {
						fmt.Println("不识别的仓位，页数：", i, "交易员：", traderNum)
					}

					if "BUY" != item.Side && "SELL" != item.Side {
						fmt.Println("不识别的方向，页数：", i, "交易员：", traderNum)
					}

					resData = append(resData, &entity.NewBinanceTradeHistory{
						Time:                item.Time,
						Symbol:              item.Symbol,
						Side:                item.Side,
						PositionSide:        item.PositionSide,
						Price:               item.Price,
						Fee:                 item.Fee,
						FeeAsset:            item.FeeAsset,
						Quantity:            item.Quantity,
						QuantityAsset:       item.QuantityAsset,
						RealizedProfit:      item.RealizedProfit,
						RealizedProfitAsset: item.RealizedProfitAsset,
						BaseAsset:           item.BaseAsset,
						Qty:                 item.Qty,
						ActiveBuy:           tmpActiveBuy,
					})
				}
			} else {
				fmt.Println("类型断言失败，无法还原数据，页数：", i, "交易员：", traderNum)
				return nil, err
			}
		} else {
			fmt.Println("dataMap不包含，页数：", i, "交易员：", traderNum)
			return nil, err
		}
	}

	return resData, nil
}

// compareBinanceTradeHistoryPageOne 拉取的binance数据细节
func (s *sBinanceTraderHistory) compareBinanceTradeHistoryPageOne(compareMax int64, traderNum uint64, binanceTradeHistoryNewestGroup []*entity.NewBinanceTradeHistory) (ipMapNeedWait map[string]bool, newData []*binanceTradeHistoryDataList, compareResDiff bool, err error) {
	// 试探开始
	var (
		tryLimit            = 3
		binanceTradeHistory []*binanceTradeHistoryDataList
	)

	ipMapNeedWait = make(map[string]bool, 0)
	newData = make([]*binanceTradeHistoryDataList, 0)
	for i := 1; i <= tryLimit; i++ {
		if 0 < s.ips.Size() { // 代理是否不为空
			var (
				ok    = false
				retry = true
			)

			// todo 因为是map，遍历时的第一次，可能一直会用某一条代理信息
			s.ips.Iterator(func(k int, v string) bool {
				ipMapNeedWait[v] = true
				binanceTradeHistory, retry, err = s.requestProxyBinanceTradeHistory(v, 1, compareMax, traderNum)
				if nil != err {
					return true
				}

				if retry {
					return true
				}

				ok = true
				return false
			})

			if ok {
				break
			}
		} else {
			fmt.Println("ip无可用")
			//binanceTradeHistory, err = s.requestBinanceTradeHistory(1, compareMax, traderNum)
			//if nil != err {
			//	return false, err
			//}
			return ipMapNeedWait, newData, false, nil
		}
	}

	// 对比
	if len(binanceTradeHistory) != len(binanceTradeHistoryNewestGroup) {
		fmt.Println("无法对比，条数不同", len(binanceTradeHistory), len(binanceTradeHistoryNewestGroup))
		return ipMapNeedWait, newData, false, nil
	}
	for k, vBinanceTradeHistory := range binanceTradeHistory {
		newData = append(newData, vBinanceTradeHistory)
		if vBinanceTradeHistory.Time == binanceTradeHistoryNewestGroup[k].Time &&
			vBinanceTradeHistory.Symbol == binanceTradeHistoryNewestGroup[k].Symbol &&
			vBinanceTradeHistory.Side == binanceTradeHistoryNewestGroup[k].Side &&
			vBinanceTradeHistory.PositionSide == binanceTradeHistoryNewestGroup[k].PositionSide &&
			IsEqual(vBinanceTradeHistory.Qty, binanceTradeHistoryNewestGroup[k].Qty) && // 数量
			IsEqual(vBinanceTradeHistory.Price, binanceTradeHistoryNewestGroup[k].Price) && //价格
			IsEqual(vBinanceTradeHistory.RealizedProfit, binanceTradeHistoryNewestGroup[k].RealizedProfit) &&
			IsEqual(vBinanceTradeHistory.Quantity, binanceTradeHistoryNewestGroup[k].Quantity) &&
			IsEqual(vBinanceTradeHistory.Fee, binanceTradeHistoryNewestGroup[k].Fee) {
		} else {
			compareResDiff = true
			break
		}
	}

	return ipMapNeedWait, newData, compareResDiff, err
}

// PullAndClose 拉取binance数据
func (s *sBinanceTraderHistory) PullAndClose(ctx context.Context) {
	//var (
	//	err error
	//)
}

// ListenThenOrder 监听拉取的binance数据
func (s *sBinanceTraderHistory) ListenThenOrder(ctx context.Context) {
	// 消费者，不停读取队列数据并输出到终端
	var (
		err error
	)
	consumerPool := grpool.New()
	for {
		var (
			dataInterface interface{}
			data          []*binanceTrade
			ok            bool
		)
		if dataInterface = s.orderQueue.Pop(); dataInterface == nil {
			continue
		}

		if data, ok = dataInterface.([]*binanceTrade); !ok {
			// 处理协程
			fmt.Println("监听程序，解析队列数据错误：", dataInterface)
			continue
		}

		// 处理协程
		err = consumerPool.Add(ctx, func(ctx context.Context) {
			// 初始化一个
			order := make([]*Order, 0)
			order = append(order, &Order{
				Uid:       0,
				BaseMoney: "",
				Data:      make([]*Data, 0),
				InitOrder: 0,
				Rate:      "",
				TraderNum: 0,
			})

			// 同一个交易员的
			for _, v := range data {
				order[0].TraderNum = v.TraderNum
				order[0].Data = append(order[0].Data, &Data{
					Symbol:     v.Symbol,
					Type:       v.Type,
					Price:      v.Price,
					Side:       v.Side,
					Qty:        strconv.FormatFloat(v.QtyFloat, 'f', -1, 64),
					Proportion: "",
					Position:   v.Position,
				})

				fmt.Println(Data{
					Symbol:     v.Symbol,
					Type:       v.Type,
					Price:      v.Price,
					Side:       v.Side,
					Qty:        strconv.FormatFloat(v.QtyFloat, 'f', -1, 64),
					Proportion: "",
					Position:   v.Position,
				})
			}

			if 0 >= len(order[0].Data) {
				return
			}

			// 请求下单
			var res string
			res, err = s.requestSystemOrder(order)
			if "ok" != res {
				fmt.Println("请求下单错,结果信息：", res, err)
				for _, vData := range order[0].Data {
					fmt.Println("请求下单错误，订单信息：", vData)
				}
			}
		})

		if nil != err {
			fmt.Println(err)
		}
	}
}

type binanceTradeHistoryResp struct {
	Data *binanceTradeHistoryData
}

type binanceTradeHistoryData struct {
	Total uint64
	List  []*binanceTradeHistoryDataList
}

type binanceTradeHistoryDataList struct {
	Time                uint64
	Symbol              string
	Side                string
	Price               float64
	Fee                 float64
	FeeAsset            string
	Quantity            float64
	QuantityAsset       string
	RealizedProfit      float64
	RealizedProfitAsset string
	BaseAsset           string
	Qty                 float64
	PositionSide        string
	ActiveBuy           bool
}

type binancePositionResp struct {
	Data []*binancePositionDataList
}

type binancePositionDataList struct {
	Symbol         string
	PositionSide   string
	PositionAmount string
}

type binancePositionHistoryResp struct {
	Data *binancePositionHistoryData
}

type binancePositionHistoryData struct {
	Total uint64
	List  []*binancePositionHistoryDataList
}

type binancePositionHistoryDataList struct {
	Time   uint64
	Symbol string
	Side   string
	Opened uint64
	Closed uint64
	Status string
}

type binanceTrade struct {
	TraderNum uint64
	Time      uint64
	Symbol    string
	Type      string
	Position  string
	Side      string
	Price     string
	Qty       string
	QtyFloat  float64
}

type Data struct {
	Symbol     string `json:"symbol"`
	Type       string `json:"type"`
	Price      string `json:"price"`
	Side       string `json:"side"`
	Qty        string `json:"qty"`
	Proportion string `json:"proportion"`
	Position   string `json:"position"`
}

type Order struct {
	Uid       uint64  `json:"uid"`
	BaseMoney string  `json:"base_money"`
	Data      []*Data `json:"data"`
	InitOrder uint64  `json:"init_order"`
	Rate      string  `json:"rate"`
	TraderNum uint64  `json:"trader_num"`
}

type SendBody struct {
	Orders    []*Order `json:"orders"`
	InitOrder uint64   `json:"init_order"`
}

type ListenTraderAndUserOrderRequest struct {
	SendBody SendBody `json:"send_body"`
}

type RequestResp struct {
	Status string
}

// 请求下单接口
func (s *sBinanceTraderHistory) requestSystemOrder(Orders []*Order) (string, error) {
	var (
		resp   *http.Response
		b      []byte
		err    error
		apiUrl = "http://127.0.0.1:8125/api/binanceexchange_user/listen_trader_and_user_order_new"
	)

	// 构造请求数据
	requestBody := ListenTraderAndUserOrderRequest{
		SendBody: SendBody{
			Orders: Orders,
		},
	}

	// 序列化为JSON
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return "", err
	}

	// 创建http.Client并设置超时时间
	client := &http.Client{
		Timeout: 20 * time.Second,
	}

	// 构造http请求
	req, err := http.NewRequest("POST", apiUrl, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	resp, err = client.Do(req)
	if err != nil {
		return "", err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	var r *RequestResp
	err = json.Unmarshal(b, &r)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	return r.Status, nil
}

// 请求binance的下单历史接口
func (s *sBinanceTraderHistory) requestProxyBinanceTradeHistory(proxyAddr string, pageNumber int64, pageSize int64, portfolioId uint64) ([]*binanceTradeHistoryDataList, bool, error) {
	var (
		resp   *http.Response
		res    []*binanceTradeHistoryDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/trade-history"
	)

	proxy, err := url.Parse(proxyAddr)
	if err != nil {
		fmt.Println(err)
		return nil, true, err
	}
	netTransport := &http.Transport{
		Proxy:                 http.ProxyURL(proxy),
		MaxIdleConnsPerHost:   10,
		ResponseHeaderTimeout: time.Second * time.Duration(5),
	}
	httpClient := &http.Client{
		Timeout:   time.Second * 10,
		Transport: netTransport,
	}

	// 构造请求
	contentType := "application/json"
	data := `{"pageNumber":` + strconv.FormatInt(pageNumber, 10) + `,"pageSize":` + strconv.FormatInt(pageSize, 10) + `,portfolioId:` + strconv.FormatUint(portfolioId, 10) + `}`
	resp, err = httpClient.Post(apiUrl, contentType, strings.NewReader(data))
	if err != nil {
		fmt.Println(333, err)
		return nil, true, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(222, err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(111, err)
		return nil, true, err
	}

	var l *binanceTradeHistoryResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		return nil, true, err
	}

	if nil == l.Data {
		return res, true, nil
	}

	res = make([]*binanceTradeHistoryDataList, 0)
	if nil == l.Data.List {
		return res, false, nil
	}

	res = make([]*binanceTradeHistoryDataList, 0)
	for _, v := range l.Data.List {
		res = append(res, v)
	}

	return res, false, nil
}

// 请求binance的仓位历史接口
func (s *sBinanceTraderHistory) requestProxyBinancePositionHistory(proxyAddr string, pageNumber int64, pageSize int64, portfolioId uint64) ([]*binancePositionHistoryDataList, bool, error) {
	var (
		resp   *http.Response
		res    []*binancePositionHistoryDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/position-history"
	)

	proxy, err := url.Parse(proxyAddr)
	if err != nil {
		fmt.Println(err)
		return nil, true, err
	}
	netTransport := &http.Transport{
		Proxy:                 http.ProxyURL(proxy),
		MaxIdleConnsPerHost:   10,
		ResponseHeaderTimeout: time.Second * time.Duration(5),
	}
	httpClient := &http.Client{
		Timeout:   time.Second * 10,
		Transport: netTransport,
	}

	// 构造请求
	contentType := "application/json"
	data := `{"sort":"OPENING","pageNumber":` + strconv.FormatInt(pageNumber, 10) + `,"pageSize":` + strconv.FormatInt(pageSize, 10) + `,portfolioId:` + strconv.FormatUint(portfolioId, 10) + `}`
	resp, err = httpClient.Post(apiUrl, contentType, strings.NewReader(data))
	if err != nil {
		fmt.Println(333, err)
		return nil, true, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(222, err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(111, err)
		return nil, true, err
	}

	var l *binancePositionHistoryResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		return nil, true, err
	}

	if nil == l.Data {
		return res, true, nil
	}

	res = make([]*binancePositionHistoryDataList, 0)
	if nil == l.Data.List {
		return res, false, nil
	}

	res = make([]*binancePositionHistoryDataList, 0)
	for _, v := range l.Data.List {
		res = append(res, v)
	}

	return res, false, nil
}

// 请求binance的持有仓位历史接口，新
func (s *sBinanceTraderHistory) requestProxyBinancePositionHistoryNew(proxyAddr string, portfolioId uint64, cookie string, token string) ([]*binancePositionDataList, bool, error) {
	var (
		resp   *http.Response
		res    []*binancePositionDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-data/positions?portfolioId=" + strconv.FormatUint(portfolioId, 10)
	)

	proxy, err := url.Parse(proxyAddr)
	if err != nil {
		fmt.Println(err)
		return nil, true, err
	}
	netTransport := &http.Transport{
		Proxy:                 http.ProxyURL(proxy),
		MaxIdleConnsPerHost:   10,
		ResponseHeaderTimeout: time.Second * time.Duration(5),
	}
	httpClient := &http.Client{
		Timeout:   time.Second * 2,
		Transport: netTransport,
	}

	// 构造请求
	req, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		fmt.Println(444444, err)
		return nil, true, err
	}

	// 添加头信息
	req.Header.Set("Clienttype", "web")
	req.Header.Set("Cookie", cookie)
	req.Header.Set("Csrftoken", token)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0")

	// 构造请求
	resp, err = httpClient.Do(req)
	if err != nil {
		fmt.Println("Error making request:", err)
		fmt.Println(444444, err)
		return nil, true, err
	}

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(4444, err)
		return nil, true, err
	}

	var l *binancePositionResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		return nil, true, err
	}

	if nil == l.Data {
		return res, true, nil
	}

	res = make([]*binancePositionDataList, 0)
	for _, v := range l.Data {
		res = append(res, v)
	}

	return res, false, nil
}

// 请求binance的持有仓位历史接口，新
func (s *sBinanceTraderHistory) requestBinancePositionHistoryNew(portfolioId uint64, cookie string, token string) ([]*binancePositionDataList, bool, error) {
	var (
		resp   *http.Response
		res    []*binancePositionDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-data/positions?portfolioId=" + strconv.FormatUint(portfolioId, 10)
	)

	// 创建不验证 SSL 证书的 HTTP 客户端
	httpClient := &http.Client{
		Timeout: time.Second * 2,
	}

	// 构造请求
	req, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		return nil, true, err
	}

	// 添加头信息
	req.Header.Set("Clienttype", "web")
	req.Header.Set("Cookie", cookie)
	req.Header.Set("Csrftoken", token)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0")

	// 发送请求
	resp, err = httpClient.Do(req)
	if err != nil {
		fmt.Println("Error making request:", err)
		return nil, true, err
	}

	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(44444, err)
		}
	}(resp.Body)

	// 结果
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(4444, err)
		return nil, true, err
	}

	//fmt.Println(string(b))
	var l *binancePositionResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		return nil, true, err
	}

	if nil == l.Data {
		return res, true, nil
	}

	res = make([]*binancePositionDataList, 0)
	for _, v := range l.Data {
		res = append(res, v)
	}

	return res, false, nil
}

// 暂时弃用
func (s *sBinanceTraderHistory) requestOrder(pageNumber int64, pageSize int64, portfolioId uint64) ([]*binanceTradeHistoryDataList, error) {
	var (
		resp   *http.Response
		res    []*binanceTradeHistoryDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/trade-history"
	)

	// 构造请求
	contentType := "application/json"
	data := `{"pageNumber":` + strconv.FormatInt(pageNumber, 10) + `,"pageSize":` + strconv.FormatInt(pageSize, 10) + `,portfolioId:` + strconv.FormatUint(portfolioId, 10) + `}`
	resp, err = http.Post(apiUrl, contentType, strings.NewReader(data))
	if err != nil {
		return nil, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	//fmt.Println(string(b))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var l *binanceTradeHistoryResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	if nil == l.Data {
		return res, nil
	}

	if nil == l.Data.List {
		return res, nil
	}

	res = make([]*binanceTradeHistoryDataList, 0)
	for _, v := range l.Data.List {
		res = append(res, v)
	}

	return res, nil
}

// 暂时弃用
func (s *sBinanceTraderHistory) requestBinanceTradeHistory(pageNumber int64, pageSize int64, portfolioId uint64) ([]*binanceTradeHistoryDataList, error) {
	var (
		resp   *http.Response
		res    []*binanceTradeHistoryDataList
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/trade-history"
	)

	// 构造请求
	contentType := "application/json"
	data := `{"pageNumber":` + strconv.FormatInt(pageNumber, 10) + `,"pageSize":` + strconv.FormatInt(pageSize, 10) + `,portfolioId:` + strconv.FormatUint(portfolioId, 10) + `}`
	resp, err = http.Post(apiUrl, contentType, strings.NewReader(data))
	if err != nil {
		return nil, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	//fmt.Println(string(b))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var l *binanceTradeHistoryResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	if nil == l.Data {
		return res, nil
	}

	if nil == l.Data.List {
		return res, nil
	}

	res = make([]*binanceTradeHistoryDataList, 0)
	for _, v := range l.Data.List {
		res = append(res, v)
	}

	return res, nil
}

type binanceOrder struct {
	OrderId       int64
	ExecutedQty   string
	ClientOrderId string
	Symbol        string
	AvgPrice      string
	CumQuote      string
	Side          string
	PositionSide  string
	ClosePosition bool
	Type          string
	Status        string
}

type orderInfo struct {
	Code int64
	Msg  string
}

func requestBinanceOrder(symbol string, side string, orderType string, positionSide string, quantity string, apiKey string, secretKey string) (*binanceOrder, *orderInfo, error) {
	var (
		client       *http.Client
		req          *http.Request
		resp         *http.Response
		res          *binanceOrder
		resOrderInfo *orderInfo
		data         string
		b            []byte
		err          error
		apiUrl       = "https://fapi.binance.com/fapi/v1/order"
	)

	//fmt.Println(symbol, side, orderType, positionSide, quantity, apiKey, secretKey)
	// 时间
	now := strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)
	// 拼请求数据
	data = "symbol=" + symbol + "&side=" + side + "&type=" + orderType + "&positionSide=" + positionSide + "&newOrderRespType=" + "RESULT" + "&quantity=" + quantity + "&timestamp=" + now

	// 加密
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(data))
	signature := hex.EncodeToString(h.Sum(nil))
	// 构造请求

	req, err = http.NewRequest("POST", apiUrl, strings.NewReader(data+"&signature="+signature))
	if err != nil {
		return nil, nil, err
	}
	// 添加头信息
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-MBX-APIKEY", apiKey)

	// 请求执行
	client = &http.Client{Timeout: 3 * time.Second}
	resp, err = client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(string(b), err)
		return nil, nil, err
	}

	var o binanceOrder
	err = json.Unmarshal(b, &o)
	if err != nil {
		fmt.Println(string(b), err)
		return nil, nil, err
	}

	res = &binanceOrder{
		OrderId:       o.OrderId,
		ExecutedQty:   o.ExecutedQty,
		ClientOrderId: o.ClientOrderId,
		Symbol:        o.Symbol,
		AvgPrice:      o.AvgPrice,
		CumQuote:      o.CumQuote,
		Side:          o.Side,
		PositionSide:  o.PositionSide,
		ClosePosition: o.ClosePosition,
		Type:          o.Type,
	}

	if 0 >= res.OrderId {
		//fmt.Println(string(b))
		err = json.Unmarshal(b, &resOrderInfo)
		if err != nil {
			fmt.Println(string(b), err)
			return nil, nil, err
		}
	}

	return res, resOrderInfo, nil
}

type BinanceTraderDetailResp struct {
	Data *BinanceTraderDetailData
}

type BinanceTraderDetailData struct {
	MarginBalance string
}

// 拉取交易员交易历史
func requestBinanceTraderDetail(portfolioId uint64) (string, error) {
	var (
		resp   *http.Response
		res    string
		b      []byte
		err    error
		apiUrl = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/detail?portfolioId=" + strconv.FormatUint(portfolioId, 10)
	)

	// 构造请求
	resp, err = http.Get(apiUrl)
	if err != nil {
		return res, err
	}

	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	var l *BinanceTraderDetailResp
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	if nil == l.Data {
		return res, nil
	}

	return l.Data.MarginBalance, nil
}
