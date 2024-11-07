// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// NewBinancePositionHistory is the golang structure of table new_binance_position_history for DAO operations like Where/Data.
type NewBinancePositionHistory struct {
	g.Meta    `orm:"table:new_binance_position_history, do:true"`
	Id        interface{} // 自增id
	Closed    interface{} //
	Opened    interface{} //
	Symbol    interface{} //
	Side      interface{} //
	Status    interface{} //
	Qty       interface{} //
	CreatedAt *gtime.Time //
	UpdatedAt *gtime.Time //
}
