// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// NewUserBindTraderTwo is the golang structure of table new_user_bind_trader_two for DAO operations like Where/Data.
type NewUserBindTraderTwo struct {
	g.Meta    `orm:"table:new_user_bind_trader_two, do:true"`
	Id        interface{} // 主键id
	UserId    interface{} // 用户id
	TraderId  interface{} // 交易员id
	Amount    interface{} //
	Status    interface{} // 可用0，不可用1，待更换2
	InitOrder interface{} // 绑定是否初始化仓位
	CreatedAt *gtime.Time // 创建时间
	UpdatedAt *gtime.Time // 更新时间
	Num       interface{} //
}
