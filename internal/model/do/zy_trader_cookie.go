// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// ZyTraderCookie is the golang structure of table zy_trader_cookie for DAO operations like Where/Data.
type ZyTraderCookie struct {
	g.Meta     `orm:"table:zy_trader_cookie, do:true"`
	Id         interface{} //
	Cookie     interface{} //
	Token      interface{} //
	TraderId   interface{} //
	UpdateTime *gtime.Time // 修正时间
	CloseTime  *gtime.Time // 关闭时间
	IsOpen     interface{} //
}
