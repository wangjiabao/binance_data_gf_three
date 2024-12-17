package logic

import (
	"binance_data_gf/internal/model/do"
	"binance_data_gf/internal/model/entity"
	"binance_data_gf/internal/service"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gateio/gateapi-go/v6"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/container/gqueue"
	"github.com/gogf/gf/v2/container/gset"
	"github.com/gogf/gf/v2/container/gtype"
	"github.com/gogf/gf/v2/container/gvar"
	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/grpool"
	"github.com/gogf/gf/v2/os/gtimer"
	"github.com/gorilla/websocket"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
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
		ips *gmap.IntStrMap
	}
)

func init() {
	service.RegisterBinanceTraderHistory(New())
}

func New() *sBinanceTraderHistory {
	return &sBinanceTraderHistory{
		grpool.New(), // 这里是请求协程池子，可以配合着可并行请求binance的限制使用，来限制最大共存数，后续jobs都将排队，考虑到上层的定时任务
		gmap.NewIntStrMap(true),
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

// UpdateCoinInfo 初始化信息
func (s *sBinanceTraderHistory) UpdateCoinInfo(ctx context.Context) bool {
	// 获取代币信息
	var (
		err     error
		symbols []*entity.LhCoinSymbol
		//bscSymbols []*bscSymbolInfo
	)
	//bscSymbols, err = requestBscCoinInfo()
	//if nil != err || 0 >= len(bscSymbols) {
	//	fmt.Println("龟兔，初始化，bsc，币种：", err)
	//	return false
	//}

	//for _, vbscSymbols := range bscSymbols {
	//	err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
	//		_, err = tx.Ctx(ctx).Insert("lh_coin_symbol", &do.LhCoinSymbol{
	//			Symbol:            vbscSymbols.BaseAsset,
	//			IsOpen:            1,
	//			QuantityPrecision: vbscSymbols.QuantityPrecision,
	//			PricePrecision:    vbscSymbols.PricePrecision,
	//			Coin:              strings.ToLower(vbscSymbols.BaseAsset),
	//		})
	//		if nil != err {
	//			return err
	//		}
	//
	//		return nil
	//	})
	//	if nil != err {
	//		fmt.Println("bsc coin insert err", err, vbscSymbols)
	//		continue
	//	}
	//}

	err = g.Model("lh_coin_symbol").Ctx(ctx).Scan(&symbols)
	if nil != err || 0 >= len(symbols) {
		fmt.Println("龟兔，初始化，币种，数据库查询错误：", err)
		return false
	}
	// 处理
	for _, vSymbols := range symbols {
		symbolsMap.Set(vSymbols.Plat+vSymbols.Symbol+"USDT", vSymbols)
	}

	return true
}

// PullPlatCoinInfo 获取平台的币种信息
func (s *sBinanceTraderHistory) PullPlatCoinInfo(ctx context.Context) bool {
	// 获取代币信息
	var (
		err        error
		okxData    []*OkxCoinInfoData
		bitGetData []*BitGetCoinInfoData
		gateData   []*GateCoinInfoData
	)

	okxData, err = requestOkxCoinInfo()
	if nil != err {
		fmt.Println("okx coin info err", err)
	}
	for _, v := range okxData {
		if "USDT" != v.SettleCcy {
			continue
		}

		var (
			lotSz float64
			ctVal float64
		)
		lotSz, err = strconv.ParseFloat(v.LotSz, 64)
		if nil != err {
			fmt.Println("错误转化", err)
		}
		ctVal, err = strconv.ParseFloat(v.CtVal, 64)
		if nil != err {
			fmt.Println("错误转化，ct_val", err)
		}
		err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
			_, err = tx.Ctx(ctx).Insert("lh_coin_symbol", &do.LhCoinSymbol{
				Symbol: v.CtValCcy,
				IsOpen: 1,
				LotSz:  lotSz,
				CtVal:  ctVal,
				Plat:   "okx",
			})
			if nil != err {
				return err
			}

			return nil
		})
		if nil != err {
			fmt.Println("okx coin insert err", err, v)
			continue
		}
	}

	bitGetData, err = requestBitGetCoinInfo()
	if nil != err {
		fmt.Println("okx coin info err", err)
	}

	for _, v := range bitGetData {
		if "perpetual" != v.SymbolType {
			continue
		}

		var (
			sizeMultiplier float64
			volumePlace    uint64
		)
		sizeMultiplier, err = strconv.ParseFloat(v.SizeMultiplier, 64)
		if nil != err {
			fmt.Println("错误转化", err)
		}
		volumePlace, err = strconv.ParseUint(v.VolumePlace, 10, 64)
		if nil != err {
			fmt.Println("错误转化", err)
		}

		err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
			_, err = tx.Ctx(ctx).Insert("lh_coin_symbol", &do.LhCoinSymbol{
				Symbol:         v.BaseCoin,
				IsOpen:         1,
				Plat:           "bitget",
				SizeMultiplier: sizeMultiplier,
				VolumePlace:    volumePlace,
			})
			if nil != err {
				return err
			}

			return nil
		})
		if nil != err {
			fmt.Println("bit get coin insert err", err, v)
			continue
		}
	}

	gateData, err = requestGateCoinInfo()
	if nil != err {
		fmt.Println("okx coin info err", err)
	}
	for _, v := range gateData {
		if 5 > len(v.Name) {
			continue
		}

		tmpName := v.Name[:len(v.Name)-5]
		var (
			quantoMultiplier float64
		)
		quantoMultiplier, err = strconv.ParseFloat(v.QuantoMultiplier, 64)
		if nil != err {
			fmt.Println("错误转化，gate", err)
		}
		err = g.DB().Transaction(context.TODO(), func(ctx context.Context, tx gdb.TX) error {
			_, err = tx.Ctx(ctx).Insert("lh_coin_symbol", &do.LhCoinSymbol{
				Symbol:           tmpName,
				IsOpen:           1,
				Plat:             "gate",
				QuantoMultiplier: quantoMultiplier,
			})
			if nil != err {
				return err
			}

			return nil
		})
		if nil != err {
			fmt.Println("gate coin insert err", err, v)
			continue
		}
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

	one = getBinanceInfo(apiKey, apiSecret)
	if 0 < len(one) {
		var tmp float64
		tmp, err = strconv.ParseFloat(one, 64)
		if nil != err {
			fmt.Println("龟兔，拉取保证金，转化失败：", err)
		}

		if !lessThanOrEqualZero(tmp, baseMoneyGuiTu.Val(), 1) {
			fmt.Println("龟兔，变更保证金", tmp, baseMoneyGuiTu.Val())
			baseMoneyGuiTu.Set(tmp)
		}
	}

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

		var (
			detail string
		)
		if "binance" == vGlobalUsers.Plat {
			if 1 == vGlobalUsers.Dai {
				if 0 >= vGlobalUsers.BinanceId {
					fmt.Println("龟兔，变更保证金，用户数据错误：", vGlobalUsers)
					return true
				}

				detail, err = requestBinanceTraderDetail(uint64(vGlobalUsers.BinanceId))
				if nil != err {
					fmt.Println("龟兔，拉取保证金失败：", err, vGlobalUsers)
					return true
				}

			} else {
				detail = getBinanceInfo(vGlobalUsers.ApiKey, vGlobalUsers.ApiSecret)
			}
		} else if "okx" == vGlobalUsers.Plat {
			//if 0 >= len(vGlobalUsers.OkxId) {
			//	fmt.Println("龟兔，变更保证金，用户数据错误：", vGlobalUsers)
			//	return true
			//}
			//
			//var (
			//	okxInfo []*okxTrader
			//)
			//okxInfo, err = requestOkxTraderDetail(vGlobalUsers.OkxId)
			//if nil != err {
			//	fmt.Println("龟兔，拉取保证金失败，okx：", err, vGlobalUsers)
			//	return true
			//}
			//
			//if nil != okxInfo && 0 < len(okxInfo) {
			//	for _, vOkxTrader := range okxInfo {
			//		for _, vN := range vOkxTrader.NonPeriodicPart {
			//			if "asset" == vN.FunctionID {
			//				detail = vN.Value
			//			}
			//		}
			//	}
			//}
		} else if "bitget" == vGlobalUsers.Plat {
			//if 0 >= len(vGlobalUsers.OkxId) {
			//	fmt.Println("龟兔，变更保证金，用户数据错误：", vGlobalUsers)
			//	return true
			//}
			//
			//detail, err = requestBitGetTraderDetail(vGlobalUsers.OkxId)
			//if nil != err {
			//	fmt.Println("龟兔，拉取保证金失败，bitget：", err, vGlobalUsers)
			//	return true
			//}

		} else if "gate" == vGlobalUsers.Plat {
			//if 0 >= len(vGlobalUsers.OkxId) {
			//	fmt.Println("龟兔，变更保证金，用户数据错误：", vGlobalUsers)
			//	return true
			//}

			//if 1 == vGlobalUsers.Dai {
			//	detail, err = requestGateTraderDetail(vGlobalUsers.OkxId)
			//	if nil != err {
			//		fmt.Println("龟兔，拉取保证金失败，gate：", err, vGlobalUsers)
			//		return true
			//	}
			//} else {
			var (
				gateUser gateapi.FuturesAccount
			)
			gateUser, err = getGateContract(vGlobalUsers.ApiKey, vGlobalUsers.ApiSecret)
			if nil != err {
				fmt.Println("龟兔，拉取保证金失败，gate：", err, vGlobalUsers)
				return true
			}

			detail = gateUser.Total
			//}

		} else {
			fmt.Println("获取平台保证金，错误用户信息", vGlobalUsers)
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
				fmt.Println("测试保证金比较", tmp, baseMoneyUserAllMap.Get(int(vGlobalUsers.Id)).(float64), lessThanOrEqualZero(tmp, baseMoneyUserAllMap.Get(int(vGlobalUsers.Id)).(float64), 1))
				if !lessThanOrEqualZero(tmp, baseMoneyUserAllMap.Get(int(vGlobalUsers.Id)).(float64), 1) {
					fmt.Println("变更成功", int(vGlobalUsers.Id), tmp, tmpUserMap[vGlobalUsers.Id].Num)
					baseMoneyUserAllMap.Set(int(vGlobalUsers.Id), tmp)
				}
			}
		} else {
			fmt.Println("保证金为0", vGlobalUsers)
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
			if "binance" == vTmpUserMap.Plat {
				if 1 == vTmpUserMap.Dai {
					if 0 >= vTmpUserMap.BinanceId {
						fmt.Println("龟兔，变更保证金，用户数据错误：", vTmpUserMap)
						continue
					}

					detail, err = requestBinanceTraderDetail(uint64(vTmpUserMap.BinanceId))
					if nil != err {
						fmt.Println("龟兔，拉取保证金失败：", err, vTmpUserMap)
					}
				} else {
					detail = getBinanceInfo(vTmpUserMap.ApiKey, vTmpUserMap.ApiSecret)
				}

			} else if "okx" == vTmpUserMap.Plat {
				//if 0 >= len(vTmpUserMap.OkxId) {
				//	fmt.Println("龟兔，变更保证金，用户数据错误：", vTmpUserMap)
				//	continue
				//}
				//
				//var (
				//	okxInfo []*okxTrader
				//)
				//okxInfo, err = requestOkxTraderDetail(vTmpUserMap.OkxId)
				//if nil != err {
				//	fmt.Println("龟兔，拉取保证金失败，okx：", err, vTmpUserMap)
				//}
				//
				//if nil != okxInfo && 0 < len(okxInfo) {
				//	for _, vOkxTrader := range okxInfo {
				//		for _, vN := range vOkxTrader.NonPeriodicPart {
				//			if "asset" == vN.FunctionID {
				//				detail = vN.Value
				//			}
				//		}
				//	}
				//}
			} else if "bitget" == vTmpUserMap.Plat {
				//if 0 >= len(vTmpUserMap.OkxId) {
				//	fmt.Println("龟兔，变更保证金，用户数据错误：", vTmpUserMap)
				//	continue
				//}
				//
				//detail, err = requestBitGetTraderDetail(vTmpUserMap.OkxId)
				//if nil != err {
				//	fmt.Println("龟兔，拉取保证金失败，bitget：", err, vTmpUserMap)
				//	continue
				//}
			} else if "gate" == vTmpUserMap.Plat {
				//if 0 >= len(vTmpUserMap.OkxId) {
				//	fmt.Println("龟兔，变更保证金，用户数据错误：", vTmpUserMap)
				//	continue
				//}
				//
				//detail, err = requestGateTraderDetail(vTmpUserMap.OkxId)
				//if nil != err {
				//	fmt.Println("龟兔，拉取保证金失败， gate：", err, vTmpUserMap)
				//	continue
				//}

				var (
					gateUser gateapi.FuturesAccount
				)
				gateUser, err = getGateContract(vTmpUserMap.ApiKey, vTmpUserMap.ApiSecret)
				if nil != err {
					fmt.Println("龟兔，拉取保证金失败，gate：", err, vTmpUserMap)
				}

				detail = gateUser.Total
			} else {
				fmt.Println("初始化，错误用户信息", vTmpUserMap)
				continue
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
					if !lessThanOrEqualZero(tmp, baseMoneyUserAllMap.Get(int(vTmpUserMap.Id)).(float64), 1) {
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

				symbolMapKey := vTmpUserMap.Plat + tmpInsertData.Symbol
				if !symbolsMap.Contains(symbolMapKey) {
					fmt.Println("龟兔，新增用户，代币信息无效，信息", tmpInsertData, vTmpUserMap)
					continue
				}

				// 下单，不用计算数量，新仓位
				var (
					binanceOrderRes *binanceOrder
					orderInfoRes    *orderInfo
				)

				if "binance" == vTmpUserMap.Plat {
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
					if 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantityPrecision {
						quantity = fmt.Sprintf("%d", int64(tmpQty))
					} else {
						quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantityPrecision, 64)
					}

					quantityFloat, err = strconv.ParseFloat(quantity, 64)
					if nil != err {
						fmt.Println(err)
						continue
					}

					if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
						continue
					}

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
					//	Side:          side,
					//	PositionSide:  positionSide,
					//	ClosePosition: false,
					//	Type:          "",
					//	Status:        "",
					//}

					// 下单异常
					if 0 >= binanceOrderRes.OrderId {
						fmt.Println("binance下单错误：", orderInfoRes)
						continue
					}

					var tmpExecutedQty float64
					tmpExecutedQty, err = strconv.ParseFloat(binanceOrderRes.ExecutedQty, 64)
					if nil != err {
						fmt.Println("龟兔，新增用户，下单错误，解析错误2，信息", err, binanceOrderRes, tmpInsertData.Symbol, side, orderType, positionSide, quantity)
						continue
					}

					// 不存在新增，这里只能是开仓
					if !orderMap.Contains(tmpInsertData.Symbol + "&" + positionSide + "&" + strUserId) {
						orderMap.Set(tmpInsertData.Symbol+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
					} else {
						tmpExecutedQty += orderMap.Get(tmpInsertData.Symbol + "&" + positionSide + "&" + strUserId).(float64)
						orderMap.Set(tmpInsertData.Symbol+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
					}
				} else if "okx" == vTmpUserMap.Plat {
					//if 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).CtVal || 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).LotSz {
					//	fmt.Println("龟兔，新增用户，代币信息错误，信息", symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol), vInsertData)
					//	continue
					//}
					//
					//var (
					//	tmpQty          float64
					//	okxRes          *okxOrderInfo
					//	side            string
					//	sideLow         string
					//	symbol          = symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).Symbol
					//	positionSide    string
					//	positionSideLow string
					//	quantity        string
					//	quantityFloat   float64
					//)
					//
					//if "LONG" == tmpInsertData.PositionSide {
					//	positionSide = "LONG"
					//	side = "BUY"
					//
					//	positionSideLow = "long"
					//	sideLow = "buy"
					//} else if "SHORT" == tmpInsertData.PositionSide {
					//	positionSide = "SHORT"
					//	side = "SELL"
					//
					//	positionSideLow = "short"
					//	sideLow = "sell"
					//} else if "BOTH" == tmpInsertData.PositionSide {
					//	if math.Signbit(tmpInsertData.PositionAmount) {
					//		positionSide = "SHORT"
					//		side = "SELL"
					//
					//		positionSideLow = "short"
					//		sideLow = "sell"
					//	} else {
					//		positionSide = "LONG"
					//		side = "BUY"
					//
					//		positionSideLow = "long"
					//		sideLow = "buy"
					//	}
					//} else {
					//	fmt.Println("龟兔，新增用户，无效信息，信息", vInsertData)
					//	continue
					//}
					//tmpPositionAmount := math.Abs(tmpInsertData.PositionAmount)
					//// 本次 代单员币的数量 * (用户保证金/代单员保证金)
					//tmpQty = tmpPositionAmount * tmpUserBindTradersAmount / tmpTraderBaseMoney // 本次开单数量
					//
					//// 转化为张数=币的数量/每张币的数量
					//tmpQtyOkx := tmpQty / symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).CtVal
					//// 按张的精度转化，
					//quantityFloat = math.Round(tmpQtyOkx/symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).LotSz) * symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).LotSz
					//if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
					//	fmt.Println("龟兔，新增用户，不足，信息", tmpQty, symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol), vInsertData)
					//	continue
					//}
					//
					//quantity = strconv.FormatFloat(quantityFloat, 'f', -1, 64)
					//
					//okxRes, err = requestOkxOrder(symbol, sideLow, positionSideLow, quantity, vTmpUserMap.ApiKey, vTmpUserMap.ApiSecret, vTmpUserMap.OkxPass)
					//if nil != err {
					//	fmt.Println("初始化，okx， 下单错误", err, symbol, side, positionSide, quantity, okxRes)
					//	continue
					//}
					//
					//if "0" != okxRes.Code {
					//	fmt.Println("初始化，okx， 下单错误1", err, symbol, side, positionSide, quantity, okxRes)
					//	for _, vData := range okxRes.Data {
					//		fmt.Println("初始化，okx， 下单错误1, 明细", vData)
					//	}
					//	continue
					//}
					//
					//if 0 >= len(okxRes.Data) || 0 >= len(okxRes.Data[0].OrdId) {
					//	fmt.Println("初始化，okx， 下单错误2", err, symbol, side, positionSide, quantity, okxRes)
					//	continue
					//}
					//
					//tmpExecutedQty := quantityFloat
					//// 不存在新增，这里只能是开仓
					//if !orderMap.Contains(tmpInsertData.Symbol + "&" + positionSide + "&" + strUserId) {
					//	orderMap.Set(tmpInsertData.Symbol+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
					//} else {
					//	tmpExecutedQty += orderMap.Get(tmpInsertData.Symbol + "&" + positionSide + "&" + strUserId).(float64)
					//	orderMap.Set(tmpInsertData.Symbol+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
					//}
				} else if "bitget" == vTmpUserMap.Plat {
					//// api规定1s一单
					//if 0 > symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).VolumePlace || 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).SizeMultiplier {
					//	fmt.Println("龟兔，新增用户，代币信息错误，信息", symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol), vInsertData)
					//	continue
					//}
					//
					//var (
					//	tmpQty           float64
					//	side             string
					//	sideBitGet       string
					//	symbol           = symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).Symbol + "USDT"
					//	positionSide     string
					//	traderSideBitGet string
					//	quantity         string
					//	quantityFloat    float64
					//)
					//
					//if "LONG" == tmpInsertData.PositionSide {
					//	positionSide = "LONG"
					//	side = "BUY"
					//
					//	sideBitGet = "buy"
					//	traderSideBitGet = "open"
					//} else if "SHORT" == tmpInsertData.PositionSide {
					//	positionSide = "SHORT"
					//	side = "SELL"
					//
					//	sideBitGet = "sell"
					//	traderSideBitGet = "open"
					//} else if "BOTH" == tmpInsertData.PositionSide {
					//	if math.Signbit(tmpInsertData.PositionAmount) {
					//		positionSide = "SHORT"
					//		side = "SELL"
					//
					//		sideBitGet = "sell"
					//		traderSideBitGet = "open"
					//	} else {
					//		positionSide = "LONG"
					//		side = "BUY"
					//
					//		sideBitGet = "buy"
					//		traderSideBitGet = "open"
					//	}
					//} else {
					//	fmt.Println("龟兔，新增用户，无效信息，信息", vInsertData)
					//	continue
					//}
					//tmpPositionAmount := math.Abs(tmpInsertData.PositionAmount)
					//// 本次 代单员币的数量 * (用户保证金/代单员保证金)
					//tmpQty = tmpPositionAmount * tmpUserBindTradersAmount / tmpTraderBaseMoney // 本次开单数量
					//
					//// 按张的精度转化，
					//quantityFloat = math.Round(tmpQty/symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).SizeMultiplier) * symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).SizeMultiplier
					//if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
					//	fmt.Println("龟兔，新增用户，不足，信息", tmpQty, symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol), vInsertData)
					//	continue
					//}
					//
					//if 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).VolumePlace {
					//	quantity = fmt.Sprintf("%d", int64(quantityFloat))
					//} else {
					//	quantity = strconv.FormatFloat(quantityFloat, 'f', symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).VolumePlace, 64)
					//}
					//
					//var (
					//	bitGetRes *bitGetOrderResponse
					//)
					//bitGetRes, err = bitGetPlaceOrder(vTmpUserMap.ApiKey, vTmpUserMap.ApiSecret, vTmpUserMap.OkxPass, &bitGetOrderRequest{
					//	Symbol:      symbol,
					//	ProductType: "USDT-FUTURES",
					//	MarginMode:  "crossed",
					//	MarginCoin:  "USDT",
					//	Size:        quantity,
					//	Side:        sideBitGet,
					//	TradeSide:   traderSideBitGet,
					//	OrderType:   "market",
					//})
					//if nil != err || "00000" != bitGetRes.Code {
					//	fmt.Println("初始化，bitget， 下单错误", err, symbol, side, positionSide, quantity, bitGetRes)
					//	continue
					//}
					//
					//if 0 >= len(bitGetRes.Data.OrderId) {
					//	fmt.Println("初始化，bitget， 下单错误2", err, symbol, side, positionSide, quantity, bitGetRes)
					//	continue
					//}
					//
					//tmpExecutedQty := quantityFloat
					//// 不存在新增，这里只能是开仓
					//if !orderMap.Contains(tmpInsertData.Symbol + "&" + positionSide + "&" + strUserId) {
					//	orderMap.Set(tmpInsertData.Symbol+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
					//} else {
					//	tmpExecutedQty += orderMap.Get(tmpInsertData.Symbol + "&" + positionSide + "&" + strUserId).(float64)
					//	orderMap.Set(tmpInsertData.Symbol+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
					//}
					//
					//time.Sleep(1 * time.Second)
				} else if "gate" == vTmpUserMap.Plat {
					if 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantoMultiplier {
						fmt.Println("龟兔，新增用户，代币信息错误，信息", symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol), vInsertData)
						continue
					}

					var (
						tmpQty        float64
						gateRes       gateapi.FuturesOrder
						side          string
						symbol        = symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).Symbol + "_USDT"
						positionSide  string
						quantity      string
						quantityInt64 int64
						quantityFloat float64
						reduceOnly    bool
					)

					tmpPositionAmount := math.Abs(tmpInsertData.PositionAmount)
					// 本次 代单员币的数量 * (用户保证金/代单员保证金)
					tmpQty = tmpPositionAmount * tmpUserBindTradersAmount / tmpTraderBaseMoney // 本次开单数量

					// 转化为张数=币的数量/每张币的数量
					tmpQtyOkx := tmpQty / symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantoMultiplier
					// 按张的精度转化，
					quantityInt64 = int64(math.Round(tmpQtyOkx))
					quantityFloat = float64(quantityInt64)
					tmpExecutedQty := quantityFloat // 正数

					if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
						fmt.Println("龟兔，新增用户，不足，信息", tmpQty, symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol), vInsertData)
						continue
					}

					if "LONG" == tmpInsertData.PositionSide {
						positionSide = "LONG"
						side = "BUY"

					} else if "SHORT" == tmpInsertData.PositionSide {
						positionSide = "SHORT"
						side = "SELL"

						quantityFloat = -quantityFloat
						quantityInt64 = -quantityInt64
					} else if "BOTH" == tmpInsertData.PositionSide {
						if math.Signbit(tmpInsertData.PositionAmount) {
							positionSide = "SHORT"
							side = "SELL"

							quantityFloat = -quantityFloat
							quantityInt64 = -quantityInt64
						} else {
							positionSide = "LONG"
							side = "BUY"
						}
					} else {
						fmt.Println("龟兔，新增用户，无效信息，信息", vInsertData)
						continue
					}

					quantity = strconv.FormatFloat(quantityFloat, 'f', -1, 64)

					gateRes, err = placeOrderGate(vTmpUserMap.ApiKey, vTmpUserMap.ApiSecret, symbol, quantityInt64, reduceOnly, "")
					if nil != err {
						fmt.Println("初始化，gate， 下单错误", err, symbol, side, positionSide, quantity, gateRes)
						continue
					}

					if 0 >= gateRes.Id {
						fmt.Println("初始化，gate， 下单错误1", err, symbol, side, positionSide, quantity, gateRes)
						continue
					}

					// 不存在新增，这里只能是开仓
					if !orderMap.Contains(tmpInsertData.Symbol + "&" + positionSide + "&" + strUserId) {
						orderMap.Set(tmpInsertData.Symbol+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
					} else {
						tmpExecutedQty += orderMap.Get(tmpInsertData.Symbol + "&" + positionSide + "&" + strUserId).(float64)
						orderMap.Set(tmpInsertData.Symbol+"&"+positionSide+"&"+strUserId, tmpExecutedQty)
					}
				} else {
					fmt.Println("初始化，错误用户信息，开仓", vTmpUserMap)
					continue
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

			// todo
			if "binance" != tmpUser.Plat {
				return true
			}

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

				symbolMapKey := tmpUser.Plat + tmpInsertData.Symbol.(string)
				if !symbolsMap.Contains(symbolMapKey) {
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
				if 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantityPrecision {
					quantity = fmt.Sprintf("%d", int64(tmpQty))
				} else {
					quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantityPrecision, 64)
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

				symbolMapKey := tmpUser.Plat + tmpUpdateData.Symbol.(string)
				if !symbolsMap.Contains(symbolMapKey) {
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
				if 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantityPrecision {
					quantity = fmt.Sprintf("%d", int64(tmpQty))
				} else {
					quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantityPrecision, 64)
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

// Binance API Config
const (
	apiBaseURL   = "https://fapi.binance.com"
	wsBaseURL    = "wss://fstream.binance.com/ws/"
	apiKey       = "" // Replace with your API Key
	apiSecret    = "" // Replace with your API Key
	listenKeyURL = "/fapi/v1/listenKey"
)

var (
	listenKey = gvar.New("", true)
	conn      *websocket.Conn // WebSocket connection
)

// ListenKeyResponse represents the response from Binance API when creating or renewing a ListenKey
type ListenKeyResponse struct {
	ListenKey string `json:"listenKey"`
}

// createListenKey creates a new ListenKey for user data stream
func createListenKey() error {
	req, err := http.NewRequest("POST", apiBaseURL+listenKeyURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-MBX-APIKEY", apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("API error: %s", string(body))
	}

	var response *ListenKeyResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return err
	}

	listenKey.Set(response.ListenKey)
	return nil
}

// renewListenKey renews the ListenKey for user data stream
func renewListenKey() error {
	req, err := http.NewRequest("PUT", apiBaseURL+listenKeyURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-MBX-APIKEY", apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("API error: %s", string(body))
	}

	return nil
}

// AccountUpdateEvent represents the `ACCOUNT_UPDATE` event pushed via WebSocket
type AccountUpdateEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Time      int64  `json:"T"`
	Account   struct {
		Positions []struct {
			Symbol         string `json:"s"`
			PositionAmount string `json:"pa"`
			EntryPrice     string `json:"ep"`
			UnrealizedPnL  string `json:"up"`
			MarginType     string `json:"mt"`
			IsolatedMargin string `json:"iw"`
			PositionSide   string `json:"ps"`
		} `json:"P"`
		M string `json:"m"`
	} `json:"a"`
}

// connectWebSocket safely connects to the WebSocket and updates conn
func connectWebSocket() error {
	// Close the existing connection if open
	if conn != nil {
		err := conn.Close()
		if err != nil {
			fmt.Println("Failed to close old connection:", err)
		}
	}

	// Create a new WebSocket connection
	wsURL := wsBaseURL + listenKey.String()
	var err error
	conn, _, err = websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %v", err)
	}
	fmt.Println("WebSocket connection established.")
	return nil
}

// handlePing sends a ping message every 9 minutes
func handlePing(ctx context.Context) {
	if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
		fmt.Println("Ping failed:", err, time.Now())
	}
}

// handleWebSocketMessages listens for messages from WebSocket and processes them
func (s *sBinanceTraderHistory) handleWebSocketMessages(ctx context.Context) {
	var (
		err error
	)

	for {
		var message []byte
		_, message, err = conn.ReadMessage()
		if err != nil {
			fmt.Println("Read error:", err, time.Now())
			// 可能是23小时的更换conn
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Handle different message types (Ping, Pong, or other messages)
		switch message[0] {
		case websocket.PingMessage:
			fmt.Println("Received Ping, sending Pong", time.Now())
			if err = conn.WriteMessage(websocket.PongMessage, nil); err != nil {
				fmt.Println("Error sending Pong:", err, time.Now())
				continue
			}
		case websocket.PongMessage:
			fmt.Println("Received Pong", time.Now())
		default:
			// Parse ACCOUNT_UPDATE events
			var event *AccountUpdateEvent
			if err = json.Unmarshal(message, &event); err != nil {
				fmt.Println("Failed to parse message:", err, string(message), time.Now())
				continue
			}

			// Check event type and process account update
			if event.EventType != "ACCOUNT_UPDATE" {
				continue
			}

			fmt.Println("Received ACCOUNT_UPDATE event:", event.Account.M,
				"事件时间ms:", event.EventTime, "撮合时间ms:", event.EventTime, "数据接收时间ms:", time.Now().UnixMilli(), "时间:", time.Now())
			if event.Account.M != "ORDER" {
				continue
			}

			// 重新初始化数据
			var (
				binancePositionMapCompare map[string]*entity.TraderPosition
			)
			if 0 < len(binancePositionMap) {
				binancePositionMapCompare = make(map[string]*entity.TraderPosition, 0)
				for k, vBinancePositionMap := range binancePositionMap {
					binancePositionMapCompare[k] = vBinancePositionMap
				}
			}

			// 用于数据库更新
			insertData := make([]*do.TraderPosition, 0)
			updateData := make([]*do.TraderPosition, 0)
			// 用于下单
			orderInsertData := make([]*do.TraderPosition, 0)
			orderUpdateData := make([]*do.TraderPosition, 0)

			for _, position := range event.Account.Positions {
				fmt.Printf("Symbol: %s, Position: %s, Entry Price: %s, Unrealized PnL: %s, Position: %s\n",
					position.Symbol, position.PositionAmount, position.EntryPrice, position.UnrealizedPnL, position.PositionSide)

				// 新增
				var (
					currentAmount    float64
					currentAmountAbs float64
				)
				currentAmount, err = strconv.ParseFloat(position.PositionAmount, 64)
				if nil != err {
					fmt.Println("新，解析金额出错，信息", position, currentAmount)
				}
				currentAmountAbs = math.Abs(currentAmount) // 绝对值

				if _, ok := binancePositionMap[position.Symbol+position.PositionSide]; !ok {
					// 以下内容，当系统无此仓位时
					if "BOTH" != position.PositionSide { // 单项持仓
						// 加入数据库
						insertData = append(insertData, &do.TraderPosition{
							Symbol:         position.Symbol,
							PositionSide:   position.PositionSide,
							PositionAmount: currentAmountAbs,
						})

						tmpPositionSide := "LONG"
						if "LONG" == position.PositionSide {
							tmpPositionSide = "SHORT"
						}

						insertData = append(insertData, &do.TraderPosition{
							Symbol:         position.Symbol,
							PositionSide:   tmpPositionSide,
							PositionAmount: 0,
						})

						insertData = append(insertData, &do.TraderPosition{
							Symbol:         position.Symbol,
							PositionSide:   "BOTH",
							PositionAmount: 0,
						})

						// 下单, 0的仓位无视
						if IsEqual(currentAmountAbs, 0) {
							continue
						}

						orderInsertData = append(orderInsertData, &do.TraderPosition{
							Symbol:         position.Symbol,
							PositionSide:   position.PositionSide,
							PositionAmount: currentAmountAbs,
						})
					} else {
						// 加入数据库
						insertData = append(insertData, &do.TraderPosition{
							Symbol:         position.Symbol,
							PositionSide:   position.PositionSide,
							PositionAmount: currentAmount, // 正负数保持
						})

						insertData = append(insertData, &do.TraderPosition{
							Symbol:         position.Symbol,
							PositionSide:   "LONG",
							PositionAmount: 0,
						})

						insertData = append(insertData, &do.TraderPosition{
							Symbol:         position.Symbol,
							PositionSide:   "SHORT",
							PositionAmount: 0,
						})

						// 模拟为多空仓，下单，todo 组合式的判断应该时牢靠的
						var tmpPositionSide string
						if IsEqual(currentAmount, 0) {
							continue
						} else if math.Signbit(currentAmount) {
							// 模拟空
							tmpPositionSide = "SHORT"
							orderInsertData = append(orderInsertData, &do.TraderPosition{
								Symbol:         position.Symbol,
								PositionSide:   tmpPositionSide,
								PositionAmount: currentAmountAbs, // 变成绝对值
							})
						} else {
							// 模拟多
							tmpPositionSide = "LONG"
							orderInsertData = append(orderInsertData, &do.TraderPosition{
								Symbol:         position.Symbol,
								PositionSide:   tmpPositionSide,
								PositionAmount: currentAmountAbs, // 变成绝对值
							})
						}
					}
				} else {
					// 以下内容，当系统有此仓位时
					// 数量无变化
					if "BOTH" != position.PositionSide {
						// 无变化的仓位
						if IsEqual(currentAmountAbs, binancePositionMap[position.Symbol+position.PositionSide].PositionAmount) {
							continue
						}

						// 有变化的仓位
						updateData = append(updateData, &do.TraderPosition{
							Id:             binancePositionMap[position.Symbol+position.PositionSide].Id,
							Symbol:         position.Symbol,
							PositionSide:   position.PositionSide,
							PositionAmount: currentAmountAbs,
						})

						orderUpdateData = append(orderUpdateData, &do.TraderPosition{
							Id:             binancePositionMap[position.Symbol+position.PositionSide].Id,
							Symbol:         position.Symbol,
							PositionSide:   position.PositionSide,
							PositionAmount: currentAmountAbs,
						})
					} else {
						if IsEqual(currentAmount, binancePositionMap[position.Symbol+position.PositionSide].PositionAmount) {
							continue
						}

						updateData = append(updateData, &do.TraderPosition{
							Id:             binancePositionMap[position.Symbol+position.PositionSide].Id,
							Symbol:         position.Symbol,
							PositionSide:   position.PositionSide,
							PositionAmount: currentAmount, // 正负数保持
						})

						// 第一步：构造虚拟的上一次仓位，空或多或无
						// 这里修改一下历史仓位的信息，方便程序在后续的流程中使用，模拟both的positionAmount为正数时，修改仓位对应的多仓方向的数据，为负数时修改空仓位的数据，0时不处理
						if _, ok = binancePositionMap[position.Symbol+"SHORT"]; !ok {
							fmt.Println("新，缺少仓位SHORT，信息", binancePositionMap[position.Symbol+position.PositionSide])
							continue
						}
						if _, ok = binancePositionMap[position.Symbol+"LONG"]; !ok {
							fmt.Println("新，缺少仓位LONG，信息", binancePositionMap[position.Symbol+position.PositionSide])
							continue
						}

						var lastPositionSide string // 上次仓位
						binancePositionMapCompare[position.Symbol+"SHORT"] = &entity.TraderPosition{
							Id:             binancePositionMapCompare[position.Symbol+"SHORT"].Id,
							Symbol:         binancePositionMapCompare[position.Symbol+"SHORT"].Symbol,
							PositionSide:   binancePositionMapCompare[position.Symbol+"SHORT"].PositionSide,
							PositionAmount: 0,
							CreatedAt:      binancePositionMapCompare[position.Symbol+"SHORT"].CreatedAt,
							UpdatedAt:      binancePositionMapCompare[position.Symbol+"SHORT"].UpdatedAt,
						}
						binancePositionMapCompare[position.Symbol+"LONG"] = &entity.TraderPosition{
							Id:             binancePositionMapCompare[position.Symbol+"LONG"].Id,
							Symbol:         binancePositionMapCompare[position.Symbol+"LONG"].Symbol,
							PositionSide:   binancePositionMapCompare[position.Symbol+"LONG"].PositionSide,
							PositionAmount: 0,
							CreatedAt:      binancePositionMapCompare[position.Symbol+"LONG"].CreatedAt,
							UpdatedAt:      binancePositionMapCompare[position.Symbol+"LONG"].UpdatedAt,
						}

						if IsEqual(binancePositionMap[position.Symbol+position.PositionSide].PositionAmount, 0) { // both仓为0
							// 认为两仓都无

						} else if math.Signbit(binancePositionMap[position.Symbol+position.PositionSide].PositionAmount) {
							lastPositionSide = "SHORT"
							binancePositionMapCompare[position.Symbol+"SHORT"].PositionAmount = math.Abs(binancePositionMap[position.Symbol+position.PositionSide].PositionAmount)
						} else {
							lastPositionSide = "LONG"
							binancePositionMapCompare[position.Symbol+"LONG"].PositionAmount = math.Abs(binancePositionMap[position.Symbol+position.PositionSide].PositionAmount)
						}

						// 本次仓位
						var tmpPositionSide string
						if IsEqual(currentAmount, 0) { // 本次仓位是0
							if 0 >= len(lastPositionSide) {
								// 本次和上一次仓位都是0，应该不会走到这里
								fmt.Println("新，仓位异常逻辑，信息", binancePositionMap[position.Symbol+position.PositionSide])
								continue
							}

							// 仍为是一次完全平仓，仓位和上一次保持一致
							tmpPositionSide = lastPositionSide
						} else if math.Signbit(currentAmount) { // 判断有无符号
							// 第二步：本次仓位

							// 上次和本次相反需要平上次
							if "LONG" == lastPositionSide {
								orderUpdateData = append(orderUpdateData, &do.TraderPosition{
									Id:             binancePositionMap[position.Symbol+position.PositionSide].Id,
									Symbol:         position.Symbol,
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
									Id:             binancePositionMap[position.Symbol+position.PositionSide].Id,
									Symbol:         position.Symbol,
									PositionSide:   lastPositionSide,
									PositionAmount: float64(0),
								})
							}

							tmpPositionSide = "LONG"
						}

						orderUpdateData = append(orderUpdateData, &do.TraderPosition{
							Id:             binancePositionMap[position.Symbol+position.PositionSide].Id,
							Symbol:         position.Symbol,
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
				fmt.Println("计算后新增的仓位：", vIBinancePosition.Symbol.(string), vIBinancePosition.PositionAmount.(float64), vIBinancePosition.PositionSide.(string))
			}

			// 更新仓位数据
			for _, vUBinancePosition := range updateData {
				binancePositionMap[vUBinancePosition.Symbol.(string)+vUBinancePosition.PositionSide.(string)] = &entity.TraderPosition{
					Id:             vUBinancePosition.Id.(uint),
					Symbol:         vUBinancePosition.Symbol.(string),
					PositionSide:   vUBinancePosition.PositionSide.(string),
					PositionAmount: vUBinancePosition.PositionAmount.(float64),
				}

				fmt.Println("计算后变更的仓位：", vUBinancePosition.Symbol.(string), vUBinancePosition.PositionAmount.(float64), vUBinancePosition.PositionSide.(string))
			}

			wg := sync.WaitGroup{}
			// 遍历跟单者
			tmpTraderBaseMoney := baseMoneyGuiTu.Val()

			globalUsers.Iterator(func(k interface{}, v interface{}) bool {
				tmpUser := v.(*entity.NewUser)

				// todo
				if "binance" != tmpUser.Plat && "gate" != tmpUser.Plat {
					return true
				}

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
					fmt.Println("龟兔，用户的信息无效了，信息", tmpUser)
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

					symbolMapKey := tmpUser.Plat + tmpInsertData.Symbol.(string)
					if !symbolsMap.Contains(symbolMapKey) {
						fmt.Println("龟兔，代币信息无效，信息", tmpInsertData, tmpUser)
						continue
					}

					if "binance" == tmpUser.Plat {

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
						if 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantityPrecision {
							quantity = fmt.Sprintf("%d", int64(tmpQty))
						} else {
							quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantityPrecision, 64)
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
							//	Side:          side,
							//	PositionSide:  positionSide,
							//	ClosePosition: false,
							//	Type:          "",
							//	Status:        "",
							//}

							// 下单异常
							if 0 >= binanceOrderRes.OrderId {
								fmt.Println(orderInfoRes)
								return
							}

							var tmpExecutedQty float64
							tmpExecutedQty, err = strconv.ParseFloat(binanceOrderRes.ExecutedQty, 64)
							if nil != err {
								fmt.Println("龟兔，下单错误，解析错误2，信息", err, tmpInsertData.Symbol.(string), side, orderType, positionSide, quantity, binanceOrderRes)
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
							fmt.Println("龟兔，添加下单任务异常，新增仓位，错误信息：", err, tmpInsertData, tmpUser)
						}

					} else if "gate" == tmpUser.Plat {
						if 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantoMultiplier {
							fmt.Println("龟兔，代币信息错误，信息", symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol), vInsertData)
							continue
						}

						var (
							tmpQty        float64
							gateRes       gateapi.FuturesOrder
							side          string
							symbol        = symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).Symbol + "_USDT"
							positionSide  string
							quantity      string
							quantityInt64 int64
							quantityFloat float64
							reduceOnly    bool
						)

						// 本次 代单员币的数量 * (用户保证金/代单员保证金)
						tmpQty = tmpInsertData.PositionAmount.(float64) * tmpUserBindTradersAmount / tmpTraderBaseMoney // 本次开单数量

						// 转化为张数=币的数量/每张币的数量
						tmpQtyOkx := tmpQty / symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantoMultiplier
						// 按张的精度转化，
						quantityInt64 = int64(math.Round(tmpQtyOkx))
						quantityFloat = float64(quantityInt64)
						tmpExecutedQty := quantityFloat // 正数

						if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
							fmt.Println("龟兔，不足，信息", tmpQty, symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol), tmpInsertData)
							continue
						}

						if "LONG" == tmpInsertData.PositionSide {
							positionSide = "LONG"
							side = "BUY"

						} else if "SHORT" == tmpInsertData.PositionSide {
							positionSide = "SHORT"
							side = "SELL"

							quantityFloat = -quantityFloat
							quantityInt64 = -quantityInt64
						} else {
							fmt.Println("龟兔，新增用户，无效信息，信息", vInsertData)
							continue
						}

						quantity = strconv.FormatFloat(quantityFloat, 'f', -1, 64)

						wg.Add(1)
						err = s.pool.Add(ctx, func(ctx context.Context) {
							defer wg.Done()

							gateRes, err = placeOrderGate(tmpUser.ApiKey, tmpUser.ApiSecret, symbol, quantityInt64, reduceOnly, "")
							if nil != err {
								fmt.Println("初始化，gate， 下单错误", err, symbol, side, positionSide, quantity, gateRes)
							}

							if 0 >= gateRes.Id {
								fmt.Println("初始化，gate， 下单错误1", err, symbol, side, positionSide, quantity, gateRes)
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
							fmt.Println("龟兔，添加下单任务异常，新增仓位，错误信息：", err, tmpInsertData, tmpUser)
						}

					} else {
						fmt.Println("错误的平台，下单", tmpUser, tmpInsertData)
					}
				}

				// 修改仓位
				for _, vUpdateData := range orderUpdateData {
					tmpUpdateData := vUpdateData
					if _, ok := binancePositionMapCompare[vUpdateData.Symbol.(string)+vUpdateData.PositionSide.(string)]; !ok {
						fmt.Println("龟兔，添加下单任务异常，修改仓位，错误信息：", err, vUpdateData, tmpUser)
						continue
					}
					lastPositionData := binancePositionMapCompare[vUpdateData.Symbol.(string)+vUpdateData.PositionSide.(string)]

					symbolMapKey := tmpUser.Plat + tmpUpdateData.Symbol.(string)
					if !symbolsMap.Contains(symbolMapKey) {
						fmt.Println("龟兔，代币信息无效，信息", tmpUpdateData, tmpUser)
						continue
					}

					if "binance" == tmpUser.Plat {
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
						if 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantityPrecision {
							quantity = fmt.Sprintf("%d", int64(tmpQty))
						} else {
							quantity = strconv.FormatFloat(tmpQty, 'f', symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantityPrecision, 64)
						}

						quantityFloat, err = strconv.ParseFloat(quantity, 64)
						if nil != err {
							fmt.Println(err)
							continue
						}

						if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
							continue
						}

						//fmt.Println("浮点测试", quantity)
						wg.Add(1)
						err = s.pool.Add(ctx, func(ctx context.Context) {
							defer wg.Done()

							// 下单，不用计算数量，新仓位
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
							//	Side:          side,
							//	PositionSide:  positionSide,
							//	ClosePosition: false,
							//	Type:          "",
							//	Status:        "",
							//}

							// 下单异常
							if 0 >= binanceOrderRes.OrderId {
								fmt.Println(orderInfoRes)
								return // 返回
							}

							var tmpExecutedQty float64
							tmpExecutedQty, err = strconv.ParseFloat(binanceOrderRes.ExecutedQty, 64)
							if nil != err {
								fmt.Println("龟兔，下单错误，解析错误2，信息", err, tmpUpdateData.Symbol.(string), side, orderType, positionSide, quantity, binanceOrderRes)
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
							fmt.Println("新，添加下单任务异常，修改仓位，错误信息：", err, vUpdateData, tmpUser)
						}
					} else if "gate" == tmpUser.Plat {
						if 0 >= symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantoMultiplier {
							fmt.Println("龟兔，代币信息错误，信息", symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol), tmpUpdateData)
							continue
						}

						var (
							tmpQty         float64
							gateRes        gateapi.FuturesOrder
							side           string
							symbol         = symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).Symbol + "_USDT"
							positionSide   string
							quantity       string
							quantityInt64  int64
							quantityFloat  float64
							tmpExecutedQty float64
							reduceOnly     bool
							closePosition  string
						)

						if lessThanOrEqualZero(tmpUpdateData.PositionAmount.(float64), 0, 1e-7) {
							fmt.Println("龟兔，完全平仓：", tmpUpdateData)
							// 全平仓
							reduceOnly = true
							if "LONG" == tmpUpdateData.PositionSide {
								positionSide = "LONG"
								side = "SELL"

								closePosition = "close_long"
							} else if "SHORT" == tmpUpdateData.PositionSide {
								positionSide = "SHORT"
								side = "BUY"

								closePosition = "close_short"
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

							tmpQty = 0
							quantityInt64 = 0
							quantityFloat = 0

							// 剩余仓位
							tmpExecutedQty = orderMap.Get(tmpUpdateData.Symbol.(string) + "&" + tmpUpdateData.PositionSide.(string) + "&" + strUserId).(float64)
						} else if lessThanOrEqualZero(lastPositionData.PositionAmount, tmpUpdateData.PositionAmount.(float64), 1e-7) {
							fmt.Println("龟兔，追加仓位：", tmpUpdateData, lastPositionData)
							// 本次减去上一次
							tmpQty = (tmpUpdateData.PositionAmount.(float64) - lastPositionData.PositionAmount) * tmpUserBindTradersAmount / tmpTraderBaseMoney // 本次开单数量

							// 转化为张数=币的数量/每张币的数量
							tmpQtyOkx := tmpQty / symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol).QuantoMultiplier
							// 按张的精度转化，
							quantityInt64 = int64(math.Round(tmpQtyOkx))
							quantityFloat = float64(quantityInt64)
							tmpExecutedQty = quantityFloat // 正数

							if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
								fmt.Println("龟兔，不足，信息", tmpQty, symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol), tmpUpdateData)
								continue
							}

							// 本次加仓 代单员币的数量 * (用户保证金/代单员保证金)
							if "LONG" == tmpUpdateData.PositionSide {
								positionSide = "LONG"
								side = "BUY"
							} else if "SHORT" == tmpUpdateData.PositionSide {
								positionSide = "SHORT"
								side = "SELL"

								quantityFloat = -quantityFloat
								quantityInt64 = -quantityInt64
							} else {
								fmt.Println("龟兔，无效信息，信息", tmpUpdateData)
								continue
							}

							quantity = strconv.FormatFloat(quantityFloat, 'f', -1, 64)

						} else if lessThanOrEqualZero(tmpUpdateData.PositionAmount.(float64), lastPositionData.PositionAmount, 1e-7) {
							fmt.Println("龟兔，部分平仓：", tmpUpdateData, lastPositionData)
							reduceOnly = true

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

							// 按张的精度转化，
							quantityInt64 = int64(tmpQty)
							quantityFloat = float64(quantityInt64)
							tmpExecutedQty = quantityFloat // 正数

							if lessThanOrEqualZero(quantityFloat, 0, 1e-7) {
								fmt.Println("龟兔，不足，信息", tmpQty, symbolsMap.Get(symbolMapKey).(*entity.LhCoinSymbol), tmpUpdateData)
								continue
							}

							// 部分平仓
							if "LONG" == tmpUpdateData.PositionSide {
								positionSide = "LONG"
								side = "SELL"

								quantityFloat = -quantityFloat
								quantityInt64 = -quantityInt64
							} else if "SHORT" == tmpUpdateData.PositionSide {
								positionSide = "SHORT"
								side = "BUY"

							} else {
								fmt.Println("龟兔，无效信息，信息", tmpUpdateData)
								continue
							}

						} else {
							fmt.Println("龟兔，分析仓位无效，信息", lastPositionData, tmpUpdateData)
							continue
						}

						wg.Add(1)
						err = s.pool.Add(ctx, func(ctx context.Context) {
							defer wg.Done()
							gateRes, err = placeOrderGate(tmpUser.ApiKey, tmpUser.ApiSecret, symbol, quantityInt64, reduceOnly, closePosition)
							if nil != err {
								fmt.Println("初始化，gate， 下单错误", err, symbol, side, positionSide, quantityInt64, quantity, gateRes)
							}

							if 0 >= gateRes.Id {
								fmt.Println("初始化，gate， 下单错误1", err, symbol, side, positionSide, quantityInt64, quantity, gateRes)
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
							fmt.Println("新，添加下单任务异常，修改仓位，错误信息：", err, vUpdateData, tmpUser)
						}

					} else {
						fmt.Println("错误的平台，下单", tmpUser, tmpUpdateData)
					}
				}

				return true
			})

			// 回收协程
			wg.Wait()

		}
	}
}

// PullAndOrderBinanceByApi pulls binance data and orders
func (s *sBinanceTraderHistory) PullAndOrderBinanceByApi(ctx context.Context) {
	var (
		err             error
		binancePosition []*BinancePosition
	)

	binancePosition = getBinancePositionInfo(apiKey, apiSecret)
	if nil == binancePosition {
		fmt.Println("错误查询仓位")
		return
	}

	// 用于数据库更新
	insertData := make([]*do.TraderPosition, 0)

	for _, position := range binancePosition {
		//fmt.Println("初始化：", position.Symbol, position.PositionAmt, position.PositionSide)

		// 新增
		var (
			currentAmount    float64
			currentAmountAbs float64
		)
		currentAmount, err = strconv.ParseFloat(position.PositionAmt, 64)
		if nil != err {
			fmt.Println("新，解析金额出错，信息", position, currentAmount)
		}
		currentAmountAbs = math.Abs(currentAmount) // 绝对值

		// 下单, 0的仓位无视
		//if IsEqual(currentAmountAbs, 0) {
		//	continue
		//}

		if _, ok := binancePositionMap[position.Symbol+position.PositionSide]; !ok {
			// 以下内容，当系统无此仓位时
			if "BOTH" != position.PositionSide { // 单项持仓
				// 加入数据库
				insertData = append(insertData, &do.TraderPosition{
					Symbol:         position.Symbol,
					PositionSide:   position.PositionSide,
					PositionAmount: currentAmountAbs,
				})

			} else {
				// 加入数据库
				insertData = append(insertData, &do.TraderPosition{
					Symbol:         position.Symbol,
					PositionSide:   position.PositionSide,
					PositionAmount: currentAmount, // 正负数保持
				})
			}
		} else {
			fmt.Println("已存在数据")
		}
	}

	if 0 < len(insertData) {
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
	}

	for _, vBinancePositionMap := range binancePositionMap {
		if _, ok := binancePositionMap[vBinancePositionMap.Symbol+"BOTH"]; !ok {
			binancePositionMap[vBinancePositionMap.Symbol+"BOTH"] = &entity.TraderPosition{
				Symbol:         vBinancePositionMap.Symbol,
				PositionSide:   "BOTH",
				PositionAmount: 0,
			}
		}
		if _, ok := binancePositionMap[vBinancePositionMap.Symbol+"LONG"]; !ok {
			binancePositionMap[vBinancePositionMap.Symbol+"LONG"] = &entity.TraderPosition{
				Symbol:         vBinancePositionMap.Symbol,
				PositionSide:   "LONG",
				PositionAmount: 0,
			}
		}
		if _, ok := binancePositionMap[vBinancePositionMap.Symbol+"SHORT"]; !ok {
			binancePositionMap[vBinancePositionMap.Symbol+"SHORT"] = &entity.TraderPosition{
				Symbol:         vBinancePositionMap.Symbol,
				PositionSide:   "SHORT",
				PositionAmount: 0,
			}
		}
	}

	//for _, vBinancePositionMap := range binancePositionMap {
	//	fmt.Println("计算后新增的仓位：", vBinancePositionMap.Symbol, vBinancePositionMap.PositionAmount, vBinancePositionMap.PositionSide)
	//}

	// Refresh listen key every 29 minutes
	handleRenewListenKey := func(ctx context.Context) {
		err := renewListenKey()
		if err != nil {
			fmt.Println("Error renewing listen key:", err)
		}
	}
	gtimer.AddSingleton(ctx, time.Minute*29, handleRenewListenKey)

	// Create listen key and connect to WebSocket
	err = createListenKey()
	if err != nil {
		fmt.Println("Error creating listen key:", err)
		return
	}

	// Connect WebSocket initially
	err = connectWebSocket()
	if err != nil {
		fmt.Println("Error connecting WebSocket:", err)
		return
	}

	// Heartbeat mechanism (send Ping every 9 minutes)
	gtimer.AddSingleton(ctx, time.Minute*9, handlePing)

	// Schedule WebSocket reconnection every 24 hours
	handleReconnect := func(ctx context.Context) {
		// Create listen key and connect to WebSocket
		err = createListenKey()
		if err != nil {
			fmt.Println("Error creating listen key:", err)
			return
		}

		// Reconnect WebSocket after 24 hours
		err = connectWebSocket()
		if err != nil {
			fmt.Println("Error reconnecting WebSocket:", err)
		}
	}
	gtimer.AddSingleton(ctx, time.Hour*23, handleReconnect)

	defer func(conn *websocket.Conn) {
		err = conn.Close()
		if err != nil {

		}
	}(conn)

	// Listen for WebSocket messages
	s.handleWebSocketMessages(ctx)
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

func requestPlatOrder(plat string, symbol string, side string, orderType string, positionSide string, quantity string, apiKey string, secretKey string) (*binanceOrder, *orderInfo, error) {
	var (
		err error
	)

	if "binance" == plat {
		return requestBinanceOrder(symbol, side, orderType, positionSide, quantity, apiKey, secretKey)
	}

	return nil, nil, err
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

// Asset 代表单个资产的保证金信息
type Asset struct {
	TotalMarginBalance string `json:"totalMarginBalance"` // 资产余额
}

// BinancePosition 代表单个头寸（持仓）信息
type BinancePosition struct {
	Symbol                 string `json:"symbol"`                 // 交易对
	InitialMargin          string `json:"initialMargin"`          // 当前所需起始保证金(基于最新标记价格)
	MaintMargin            string `json:"maintMargin"`            // 维持保证金
	UnrealizedProfit       string `json:"unrealizedProfit"`       // 持仓未实现盈亏
	PositionInitialMargin  string `json:"positionInitialMargin"`  // 持仓所需起始保证金(基于最新标记价格)
	OpenOrderInitialMargin string `json:"openOrderInitialMargin"` // 当前挂单所需起始保证金(基于最新标记价格)
	Leverage               string `json:"leverage"`               // 杠杆倍率
	Isolated               bool   `json:"isolated"`               // 是否是逐仓模式
	EntryPrice             string `json:"entryPrice"`             // 持仓成本价
	MaxNotional            string `json:"maxNotional"`            // 当前杠杆下用户可用的最大名义价值
	BidNotional            string `json:"bidNotional"`            // 买单净值，忽略
	AskNotional            string `json:"askNotional"`            // 卖单净值，忽略
	PositionSide           string `json:"positionSide"`           // 持仓方向 (BOTH, LONG, SHORT)
	PositionAmt            string `json:"positionAmt"`            // 持仓数量
	UpdateTime             int64  `json:"updateTime"`             // 更新时间
}

// BinanceResponse 包含多个仓位和账户信息
type BinanceResponse struct {
	Positions []*BinancePosition `json:"positions"` // 仓位信息
}

// 生成签名
func generateSignature(apiS string, params url.Values) string {
	// 将请求参数编码成 URL 格式的字符串
	queryString := params.Encode()

	// 生成签名
	mac := hmac.New(sha256.New, []byte(apiS))
	mac.Write([]byte(queryString)) // 用 API Secret 生成签名
	return hex.EncodeToString(mac.Sum(nil))
}

// 获取币安服务器时间
func getBinanceServerTime() int64 {
	urlTmp := "https://api.binance.com/api/v3/time"
	resp, err := http.Get(urlTmp)
	if err != nil {
		fmt.Println("Error getting server time:", err)
		return 0
	}
	defer resp.Body.Close()

	var serverTimeResponse struct {
		ServerTime int64 `json:"serverTime"`
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return 0
	}
	if err := json.Unmarshal(body, &serverTimeResponse); err != nil {
		fmt.Println("Error unmarshaling server time:", err)
		return 0
	}

	return serverTimeResponse.ServerTime
}

// 获取账户信息
func getBinanceInfo(apiK, apiS string) string {
	// 请求的API地址
	endpoint := "/fapi/v2/account"
	baseURL := "https://fapi.binance.com"

	// 获取当前时间戳（使用服务器时间避免时差问题）
	serverTime := getBinanceServerTime()
	if serverTime == 0 {
		return ""
	}
	timestamp := strconv.FormatInt(serverTime, 10)

	// 设置请求参数
	params := url.Values{}
	params.Set("timestamp", timestamp)
	params.Set("recvWindow", "5000") // 设置接收窗口

	// 生成签名
	signature := generateSignature(apiS, params)

	// 将签名添加到请求参数中
	params.Set("signature", signature)

	// 构建完整的请求URL
	requestURL := baseURL + endpoint + "?" + params.Encode()

	// 创建请求
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		return ""
	}

	// 添加请求头
	req.Header.Add("X-MBX-APIKEY", apiK)

	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return ""
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response:", err)
		return ""
	}

	// 解析响应
	var o *Asset
	err = json.Unmarshal(body, &o)
	if err != nil {
		fmt.Println("Error unmarshalling response:", err)
		return ""
	}

	// 返回资产余额
	return o.TotalMarginBalance
}

// 获取账户信息
func getBinancePositionInfo(apiK, apiS string) []*BinancePosition {
	// 请求的API地址
	endpoint := "/fapi/v2/account"
	baseURL := "https://fapi.binance.com"

	// 获取当前时间戳（使用服务器时间避免时差问题）
	serverTime := getBinanceServerTime()
	if serverTime == 0 {
		return nil
	}
	timestamp := strconv.FormatInt(serverTime, 10)

	// 设置请求参数
	params := url.Values{}
	params.Set("timestamp", timestamp)
	params.Set("recvWindow", "5000") // 设置接收窗口

	// 生成签名
	signature := generateSignature(apiS, params)

	// 将签名添加到请求参数中
	params.Set("signature", signature)

	// 构建完整的请求URL
	requestURL := baseURL + endpoint + "?" + params.Encode()

	// 创建请求
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		return nil
	}

	// 添加请求头
	req.Header.Add("X-MBX-APIKEY", apiK)

	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return nil
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response:", err)
		return nil
	}

	// 解析响应
	var o *BinanceResponse
	err = json.Unmarshal(body, &o)
	if err != nil {
		fmt.Println("Error unmarshalling response:", err)
		return nil
	}

	// 返回资产余额
	return o.Positions
}

func okxGenerateSignature(payload, secretKey string) string {
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(payload))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

type okxOrderInfo struct {
	Code    string              `json:"code"`    // 主请求返回状态码，"0" 表示成功
	Msg     string              `json:"msg"`     // 主请求的消息，通常为空
	Data    []*okxOrderInfoData `json:"data"`    // 批量订单的结果列表
	InTime  string              `json:"inTime"`  // 请求进入时间戳
	OutTime string              `json:"outTime"` // 请求完成时间戳
}

type okxOrderInfoData struct {
	ClOrdId string `json:"clOrdId"` // 客户端订单 ID
	OrdId   string `json:"ordId"`   // 服务器生成的订单 ID
	Tag     string `json:"tag"`     // 标签，通常为空
	Ts      string `json:"ts"`      // 时间戳，订单生成的时间
	SCode   string `json:"sCode"`   // 订单状态码，"0" 表示成功
	SMsg    string `json:"sMsg"`    // 状态消息，通常为空或错误原因
}

func requestOkxOrder(symbol string, side string, positionSide string, quantity string, apiKey string, secretKey string, pass string) (*okxOrderInfo, error) {
	var (
		client   *http.Client
		req      *http.Request
		resp     *http.Response
		jsonData []byte
		b        []byte
		err      error
	)
	// 请求信息
	method := "POST"
	baseURL := "https://www.okx.com"
	requestPath := "/api/v5/trade/order"
	body := map[string]interface{}{
		"instId":  symbol + "-USDT-SWAP",
		"tdMode":  "cross",
		"posSide": positionSide,
		"side":    side,
		"ordType": "market",
		"sz":      quantity,
	}

	// 转换 body 为 JSON
	jsonData, err = json.Marshal(body)
	if err != nil {
		fmt.Println("Error encoding JSON:", err)
		return nil, err
	}

	// 获取当前时间戳
	timestamp := time.Now().UTC().Format(time.RFC3339)

	// 构建签名字符串
	signaturePayload := timestamp + method + requestPath + string(jsonData)
	signature := okxGenerateSignature(signaturePayload, secretKey)

	// 创建请求
	req, err = http.NewRequest(method, baseURL+requestPath, bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return nil, err
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("OK-ACCESS-KEY", apiKey)
	req.Header.Set("OK-ACCESS-SIGN", signature)
	req.Header.Set("OK-ACCESS-TIMESTAMP", timestamp)
	req.Header.Set("OK-ACCESS-PASSPHRASE", pass)

	// 请求执行
	client = &http.Client{Timeout: 3 * time.Second}
	resp, err = client.Do(req)
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
		fmt.Println(string(b), err)
		return nil, err
	}

	var o *okxOrderInfo
	err = json.Unmarshal(b, &o)
	if err != nil {
		fmt.Println(string(b), err)
		return nil, err
	}

	return o, nil
}

// OrderRequest 定义下单的请求结构体
type bitGetOrderRequest struct {
	Symbol      string `json:"symbol"`
	ProductType string `json:"productType"`
	MarginMode  string `json:"marginMode"`
	MarginCoin  string `json:"marginCoin"`
	Size        string `json:"size"`
	Side        string `json:"side"`
	TradeSide   string `json:"tradeSide"`
	OrderType   string `json:"orderType"`
}

// OrderResponse 定义响应结构体
type bitGetOrderResponse struct {
	Code    string `json:"code"`
	Message string `json:"msg"`
	Data    struct {
		ClientOid string `json:"clientOid"`
		OrderId   string `json:"orderId"`
	} `json:"data"`
}

// bitGetGenerateSignature 生成 Bitget API 签名
func bitGetGenerateSignature(secret, method, requestPath, timestamp, body string) (string, error) {
	message := timestamp + method + requestPath + body
	mac := hmac.New(sha256.New, []byte(secret))
	_, err := mac.Write([]byte(message))
	if err != nil {
		return "", fmt.Errorf("failed to write message for HMAC: %v", err)
	}
	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

// placeOrder 发送下单请求
func bitGetPlaceOrder(apiKey, apiSecret, passphrase string, order *bitGetOrderRequest) (*bitGetOrderResponse, error) {
	baseURL := "https://api.bitget.com"
	method := "POST"
	requestPath := "/api/v2/mix/order/place-order"
	timestamp := fmt.Sprintf("%d", time.Now().UnixMilli())

	// 序列化请求体
	bodyBytes, err := json.Marshal(order)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize order request: %v", err)
	}
	body := string(bodyBytes)

	// 生成签名
	signature, err := bitGetGenerateSignature(apiSecret, method, requestPath, timestamp, body)
	if err != nil {
		return nil, fmt.Errorf("failed to generate signature: %v", err)
	}

	// 构造 HTTP 请求
	urlReq := baseURL + requestPath
	req, err := http.NewRequest(method, urlReq, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}

	// 设置请求头
	req.Header.Set("ACCESS-KEY", apiKey)
	req.Header.Set("ACCESS-SIGN", signature)
	req.Header.Set("ACCESS-TIMESTAMP", timestamp)
	req.Header.Set("ACCESS-PASSPHRASE", passphrase)
	req.Header.Set("locale", "zh-CN")
	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request: %v", err)
	}
	// 结果
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)

	// 读取响应
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	//fmt.Println(string(respBody))
	// 解析响应
	var orderResponse *bitGetOrderResponse
	err = json.Unmarshal(respBody, &orderResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response JSON: %v", err)
	}

	return orderResponse, nil
}

// OrderRequestGate is the request structure for placing an order
type OrderRequestGate struct {
	Contract   string `json:"contract"`
	Size       int64  `json:"size"`
	Tif        string `json:"tif"`
	AutoSize   string `json:"auto_size"`
	ReduceOnly bool   `json:"reduce_only"`
}

// OrderResponseGate is the full response structure for the order API
type OrderResponseGate struct {
	ID           int    `json:"id"`
	User         int    `json:"user"`
	Contract     string `json:"contract"`
	CreateTime   int64  `json:"create_time"`
	Size         int    `json:"size"`
	Iceberg      int    `json:"iceberg"`
	Left         int    `json:"left"`
	Price        string `json:"price"`
	FillPrice    string `json:"fill_price"`
	Mkfr         string `json:"mkfr"`
	Tkfr         string `json:"tkfr"`
	Tif          string `json:"tif"`
	Refu         int    `json:"refu"`
	IsReduceOnly bool   `json:"is_reduce_only"`
	IsClose      bool   `json:"is_close"`
	IsLiq        bool   `json:"is_liq"`
	Text         string `json:"text"`
	Status       string `json:"status"`
	FinishTime   int64  `json:"finish_time"`
	FinishAs     string `json:"finish_as"`
	StpID        int    `json:"stp_id"`
	StpAct       string `json:"stp_act"`
	AmendText    string `json:"amend_text"`
}

// generateSignatureGate 生成 API 签名
func generateSignatureGate(signatureString, apiS string) string {
	h := hmac.New(sha512.New, []byte(apiS))
	h.Write([]byte(signatureString))
	return hex.EncodeToString(h.Sum(nil))
}

// getGateContract
func getGateContract(apiK, apiS string) (gateapi.FuturesAccount, error) {
	client := gateapi.NewAPIClient(gateapi.NewConfiguration())
	// uncomment the next line if your are testing against testnet
	// client.ChangeBasePath("https://fx-api-testnet.gateio.ws/api/v4")
	ctx := context.WithValue(context.Background(),
		gateapi.ContextGateAPIV4,
		gateapi.GateAPIV4{
			Key:    apiK,
			Secret: apiS,
		},
	)

	result, _, err := client.FuturesApi.ListFuturesAccounts(ctx, "usdt")
	if err != nil {
		if e, ok := err.(gateapi.GateAPIError); ok {
			fmt.Println("gate api error: ", e.Error())
		} else {
			fmt.Println("generic error: ", err.Error())
		}
	}

	return result, nil
}

// placeOrderGate places an order on the Gate.io API with dynamic parameters
func placeOrderGate(apiK, apiS, contract string, size int64, reduceOnly bool, autoSize string) (gateapi.FuturesOrder, error) {
	client := gateapi.NewAPIClient(gateapi.NewConfiguration())
	// uncomment the next line if your are testing against testnet
	// client.ChangeBasePath("https://fx-api-testnet.gateio.ws/api/v4")
	ctx := context.WithValue(context.Background(),
		gateapi.ContextGateAPIV4,
		gateapi.GateAPIV4{
			Key:    apiK,
			Secret: apiS,
		},
	)

	order := gateapi.FuturesOrder{
		Contract: contract,
		Size:     size,
		Tif:      "ioc",
		Price:    "0",
	}

	if autoSize != "" {
		order.AutoSize = autoSize
	}

	// 如果 reduceOnly 为 true，添加到请求数据中
	if reduceOnly {
		order.ReduceOnly = reduceOnly
	}

	result, _, err := client.FuturesApi.CreateFuturesOrder(ctx, "usdt", order)

	if err != nil {
		if e, ok := err.(gateapi.GateAPIError); ok {
			fmt.Println("gate api error: ", e.Error())
		} else {
			fmt.Println("generic error: ", err.Error())
		}
	}

	return result, nil
}

//// placeOrderGate places an order on the Gate.io API with dynamic parameters
//func placeOrderGate(apiK, apiS, contract string, size int64, reduceOnly bool, autoSize string) (*OrderResponseGate, error) {
//	// 构造请求的 JSON 数据
//	orderRequest := map[string]interface{}{
//		"contract": contract, // 合约名，如 BTC_USDT
//		"size":     size,     // 合约数量
//		"tif":      "ioc",    // 有效时间：IOC（立即成交或取消）
//	}
//
//	// 如果 autoSize 不为空，则添加到请求数据中
//	if autoSize != "" {
//		orderRequest["auto_size"] = autoSize
//	}
//
//	// 如果 reduceOnly 为 true，添加到请求数据中
//	if reduceOnly {
//		orderRequest["reduce_only"] = reduceOnly
//	}
//
//	baseURL := "https://api.gateio.ws/api/v4"
//	urlTmp := fmt.Sprintf("%s/futures/usdt/orders", baseURL) // 正确路径
//
//	// 将请求体序列化为 JSON
//	body, err := json.Marshal(orderRequest)
//	if err != nil {
//		return nil, err
//	}
//
//	// 创建 HTTP 请求
//	req, err := http.NewRequest("POST", urlTmp, bytes.NewBuffer(body))
//	if err != nil {
//		return nil, err
//	}
//
//	// 获取当前时间戳（秒级）
//	timestamp := fmt.Sprintf("%d", time.Now().Unix())
//
//	// 获取请求路径，不包含域名和参数
//	parsedURL, err := url.Parse(urlTmp)
//	if err != nil {
//		return nil, err
//	}
//	path := parsedURL.Path // 确保路径正确
//
//	// 如果有查询参数，拼接查询字符串
//	queryString := "" // API 请求没有查询参数
//
//	// 请求体的 SHA512 哈希（如果请求体为空，使用空字符串的 SHA512 哈希结果）
//	hash := sha512.New()
//	if len(body) > 0 {
//		hash.Write(body) // 如果有请求体，计算其 SHA512 哈希
//	} else {
//		hash.Write([]byte("")) // 如果请求体为空，使用空字符串的哈希
//	}
//	hexHash := hex.EncodeToString(hash.Sum(nil))
//
//	// 构建签名字符串
//	signatureString := fmt.Sprintf("%s\n%s\n%s\n%s\n%s", "POST", path, queryString, hexHash, timestamp)
//
//	// 使用 API Secret 生成签名
//	signature := generateSignatureGate(signatureString, apiS)
//
//	// 设置请求头
//	req.Header.Set("KEY", apiK)
//	req.Header.Set("SIGN", signature)
//	req.Header.Set("Timestamp", timestamp)
//	req.Header.Set("Content-Type", "application/json")
//
//	// 发送请求
//	client := &http.Client{}
//	resp, err := client.Do(req)
//	if err != nil {
//		fmt.Println("Error sending request:", err)
//		return nil, err
//	}
//	defer resp.Body.Close()
//
//	// 读取并打印响应
//	bodyResp, err := ioutil.ReadAll(resp.Body)
//	if err != nil {
//		fmt.Println("Error reading response:", err)
//		return nil, err
//	}
//
//	fmt.Println(string(bodyResp))
//	var response *OrderResponseGate
//	if err := json.Unmarshal(bodyResp, &response); err != nil {
//		fmt.Println("Error unmarshaling response:", err)
//		return nil, err
//	}
//
//	// 返回响应
//	return response, nil
//}

type BinanceTraderDetailResp struct {
	Data *BinanceTraderDetailData
}

type BinanceTraderDetailData struct {
	MarginBalance string
}

// 拉取交易员交易保证金
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

type okxResponse struct {
	Code string       `json:"code"`
	Data []*okxTrader `json:"data"`
	Msg  string       `json:"msg"`
}

type okxTrader struct {
	NonPeriodicPart []*okxMetric `json:"nonPeriodicPart"`
}

type okxMetric struct {
	Desc         string `json:"desc"`
	FunctionID   string `json:"functionId"`
	LearnMoreURL string `json:"learnMoreUrl"`
	Order        int    `json:"order"`
	Title        string `json:"title"`
	Type         string `json:"type"`
	Value        string `json:"value"`
}

// 拉取okx交易员交易保证金
func requestOkxTraderDetail(portfolioId string) ([]*okxTrader, error) {
	var (
		resp   *http.Response
		res    []*okxTrader
		b      []byte
		err    error
		apiUrl = "https://www.okx.com/priapi/v5/ecotrade/public/trader/trade-data?latestNum=0&bizType=SWAP&uniqueName=" + portfolioId
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

	var l *okxResponse
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	if nil == l.Data {
		return res, nil
	}

	return l.Data, nil
}

type bitGetTraderResponse struct {
	Code string `json:"code"` // 响应代码
	Data struct {
		TotalEquity string `json:"totalEquity"` // 总资产
	} `json:"data"`
}

// 拉取bitget交易员交易保证金
func requestBitGetTraderDetail(portfolioId string) (string, error) {
	var (
		resp   *http.Response
		res    string
		b      []byte
		err    error
		apiUrl = "https://www.bitget.com/v1/trigger/trace/public/traderDetailPageV2"
	)

	// 构造请求数据
	data := map[string]interface{}{
		"languageType": 1,
		"traderUid":    portfolioId,
	}
	requestBody, err := json.Marshal(data)
	if err != nil {
		return res, fmt.Errorf("failed to marshal request body: %w", err)
	}

	// 构造 HTTP 请求
	req, err := http.NewRequest("POST", apiUrl, bytes.NewReader(requestBody))
	if err != nil {
		return res, fmt.Errorf("failed to create new request: %w", err)
	}

	// 添加 headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0") // 可根据需要自定义 User-Agent

	client := &http.Client{}
	resp, err = client.Do(req)

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

	//fmt.Println(string(b))
	var l *bitGetTraderResponse
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	return l.Data.TotalEquity, nil
}

type gateTraderResponse struct {
	Code int `json:"code"` // 响应代码
	Data struct {
		Profit struct {
			TotalInvest string `json:"total_invest"` // 总资产
		} `json:"profit"` // 响应代码
	} `json:"data"` // 数据列表
}

// 拉取gate交易员交易保证金
func requestGateTraderDetail(portfolioId string) (string, error) {
	var (
		resp   *http.Response
		res    string
		b      []byte
		err    error
		apiUrl = "https://www.gate.io/api/copytrade/copy_trading/trader/detail/" + portfolioId + "?leaderId=" + portfolioId
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

	var l *gateTraderResponse
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	return l.Data.Profit.TotalInvest, nil
}

type OkxCoinInfoRes struct {
	Data []*OkxCoinInfoData
}

type OkxCoinInfoData struct {
	Uly       string
	LotSz     string
	CtVal     string
	CtValCcy  string
	SettleCcy string
}

// 拉取okx币种
func requestOkxCoinInfo() ([]*OkxCoinInfoData, error) {
	var (
		resp   *http.Response
		res    = make([]*OkxCoinInfoData, 0)
		b      []byte
		err    error
		apiUrl = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
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

	var l *OkxCoinInfoRes
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	if nil == l.Data {
		return res, nil
	}

	return l.Data, nil
}

type BitGetCoinInfoRes struct {
	Data []*BitGetCoinInfoData
}

type BitGetCoinInfoData struct {
	Symbol         string
	BaseCoin       string
	QuoteCoin      string
	VolumePlace    string
	SizeMultiplier string
	SymbolType     string
}

// 拉取bitget币种
func requestBitGetCoinInfo() ([]*BitGetCoinInfoData, error) {
	var (
		resp   *http.Response
		res    = make([]*BitGetCoinInfoData, 0)
		b      []byte
		err    error
		apiUrl = "https://api.bitget.com/api/v2/mix/market/contracts?productType=usdt-futures"
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

	var l *BitGetCoinInfoRes
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	if nil == l.Data {
		return res, nil
	}

	return l.Data, nil
}

type GateCoinInfoData struct {
	Name             string
	QuantoMultiplier string `json:"quanto_multiplier"`
}

// 拉取gate币种
func requestGateCoinInfo() ([]*GateCoinInfoData, error) {
	var (
		resp   *http.Response
		res    = make([]*GateCoinInfoData, 0)
		b      []byte
		err    error
		apiUrl = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
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

	err = json.Unmarshal(b, &res)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	return res, nil
}

// SymbolInfo 定义交易对信息结构
type bscSymbolInfo struct {
	BaseAsset         string `json:"baseAsset"`
	Symbol            string `json:"symbol"`
	PricePrecision    int    `json:"pricePrecision"`
	QuantityPrecision int    `json:"quantityPrecision"`
}

// ExchangeInfo 定义返回的主结构
type bscExchangeInfo struct {
	Symbols []*bscSymbolInfo `json:"symbols"`
}

// 拉取binance币种
func requestBscCoinInfo() ([]*bscSymbolInfo, error) {
	var (
		resp   *http.Response
		res    = make([]*bscSymbolInfo, 0)
		b      []byte
		err    error
		apiUrl = "https://fapi.binance.com/fapi/v1/exchangeInfo"
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

	var l *bscExchangeInfo
	//fmt.Println(string(b))
	err = json.Unmarshal(b, &l)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	return l.Symbols, nil
}
