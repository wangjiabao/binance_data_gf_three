// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// NewBinancePosition is the golang structure of table new_binance_position for DAO operations like Where/Data.
type NewBinancePosition struct {
	g.Meta       `orm:"table:new_binance_position, do:true"`
	Id           interface{} // 自增id
	Symbol       interface{} //
	Side         interface{} //
	PositionSide interface{} //
	Qty          interface{} //
	Status       interface{} //
	CreatedAt    *gtime.Time //
	UpdatedAt    *gtime.Time //
}
