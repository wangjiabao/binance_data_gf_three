// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// NewUser is the golang structure of table new_user for DAO operations like Where/Data.
type NewUser struct {
	g.Meta              `orm:"table:new_user, do:true"`
	Id                  interface{} // 用户id
	Address             interface{} // 用户address
	ApiStatus           interface{} // api的可用状态：不可用2
	ApiKey              interface{} // 用户币安apikey
	ApiSecret           interface{} // 用户币安apisecret
	BindTraderStatus    interface{} // 绑定trader状态：0未绑定，1绑定
	BindTraderStatusTfi interface{} //
	UseNewSystem        interface{} //
	IsDai               interface{} //
	CreatedAt           *gtime.Time //
	UpdatedAt           *gtime.Time //
	BinanceId           interface{} //
	NeedInit            interface{} //
	Num                 interface{} //
	ReInit              interface{} //
}
