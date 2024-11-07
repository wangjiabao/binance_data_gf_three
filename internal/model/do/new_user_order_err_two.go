// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// NewUserOrderErrTwo is the golang structure of table new_user_order_err_two for DAO operations like Where/Data.
type NewUserOrderErrTwo struct {
	g.Meta        `orm:"table:new_user_order_err_two, do:true"`
	Id            interface{} // 主键id
	UserId        interface{} //
	TraderId      interface{} //
	ClientOrderId interface{} //
	OrderId       interface{} // binance系统订单号
	Symbol        interface{} //
	Side          interface{} // 买卖："SELL","BUY"
	PositionSide  interface{} // 方向: 多"LONG",空"SHORT"
	Quantity      interface{} // 下单数量
	Price         interface{} // 价格
	TraderQty     interface{} // 交易员下单数量
	OrderType     interface{} // 类型：默认MARKET市价
	ClosePosition interface{} // 是否条件全平仓
	CumQuote      interface{} // 成交金额，biance返回真实的市价交易的成交金额
	ExecutedQty   interface{} // 成交量，biance返回真实的市价交易的数量
	AvgPrice      interface{} // 平均价格，biance返回真实的市价交易价格
	HandleStatus  interface{} //
	Code          interface{} //
	Msg           interface{} //
	InitOrder     interface{} //
	Proportion    interface{} //
	CreatedAt     *gtime.Time //
	UpdatedAt     *gtime.Time //
}
