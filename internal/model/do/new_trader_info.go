// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// NewTraderInfo is the golang structure of table new_trader_info for DAO operations like Where/Data.
type NewTraderInfo struct {
	g.Meta    `orm:"table:new_trader_info, do:true"`
	Id        interface{} //
	TraderId  interface{} //
	BId       interface{} //
	BaseMoney interface{} //
	CreatedAt *gtime.Time //
	UpdatedAt *gtime.Time //
}
