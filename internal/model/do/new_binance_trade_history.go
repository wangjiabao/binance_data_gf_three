// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// NewBinanceTradeHistory is the golang structure of table new_binance_trade_history for DAO operations like Where/Data.
type NewBinanceTradeHistory struct {
	g.Meta              `orm:"table:new_binance_trade_history, do:true"`
	Id                  interface{} // 自增id
	Time                interface{} //
	Symbol              interface{} //
	Side                interface{} //
	PositionSide        interface{} //
	Price               interface{} //
	Fee                 interface{} //
	FeeAsset            interface{} //
	Quantity            interface{} //
	QuantityAsset       interface{} //
	RealizedProfit      interface{} //
	RealizedProfitAsset interface{} //
	BaseAsset           interface{} //
	Qty                 interface{} //
	ActiveBuy           interface{} //
	CreatedAt           *gtime.Time //
	UpdatedAt           *gtime.Time //
}
