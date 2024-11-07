// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
)

// LhCoinSymbol is the golang structure of table lh_coin_symbol for DAO operations like Where/Data.
type LhCoinSymbol struct {
	g.Meta            `orm:"table:lh_coin_symbol, do:true"`
	Id                interface{} //
	Coin              interface{} //
	Symbol            interface{} //
	StartTime         interface{} //
	EndTime           interface{} //
	PricePrecision    interface{} // 小数点精度
	QuantityPrecision interface{} //
	IsOpen            interface{} //
}
