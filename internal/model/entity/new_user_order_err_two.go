// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// NewUserOrderErrTwo is the golang structure for table new_user_order_err_two.
type NewUserOrderErrTwo struct {
	Id            uint        `json:"id"            ` // 主键id
	UserId        uint        `json:"userId"        ` //
	TraderId      uint        `json:"traderId"      ` //
	ClientOrderId string      `json:"clientOrderId" ` //
	OrderId       string      `json:"orderId"       ` // binance系统订单号
	Symbol        string      `json:"symbol"        ` //
	Side          string      `json:"side"          ` // 买卖："SELL","BUY"
	PositionSide  string      `json:"positionSide"  ` // 方向: 多"LONG",空"SHORT"
	Quantity      float64     `json:"quantity"      ` // 下单数量
	Price         float64     `json:"price"         ` // 价格
	TraderQty     float64     `json:"traderQty"     ` // 交易员下单数量
	OrderType     string      `json:"orderType"     ` // 类型：默认MARKET市价
	ClosePosition string      `json:"closePosition" ` // 是否条件全平仓
	CumQuote      float64     `json:"cumQuote"      ` // 成交金额，biance返回真实的市价交易的成交金额
	ExecutedQty   float64     `json:"executedQty"   ` // 成交量，biance返回真实的市价交易的数量
	AvgPrice      float64     `json:"avgPrice"      ` // 平均价格，biance返回真实的市价交易价格
	HandleStatus  uint        `json:"handleStatus"  ` //
	Code          int         `json:"code"          ` //
	Msg           string      `json:"msg"           ` //
	InitOrder     int         `json:"initOrder"     ` //
	Proportion    float64     `json:"proportion"    ` //
	CreatedAt     *gtime.Time `json:"createdAt"     ` //
	UpdatedAt     *gtime.Time `json:"updatedAt"     ` //
}
