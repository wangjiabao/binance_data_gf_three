// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// TraderPosition is the golang structure of table trader_position for DAO operations like Where/Data.
type TraderPosition struct {
	g.Meta         `orm:"table:trader_position, do:true"`
	Id             interface{} //
	Symbol         interface{} //
	PositionSide   interface{} //
	PositionAmount interface{} //
	CreatedAt      *gtime.Time //
	UpdatedAt      *gtime.Time //
}
