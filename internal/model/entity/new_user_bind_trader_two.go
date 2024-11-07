// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// NewUserBindTraderTwo is the golang structure for table new_user_bind_trader_two.
type NewUserBindTraderTwo struct {
	Id        uint        `json:"id"        ` // 主键id
	UserId    uint        `json:"userId"    ` // 用户id
	TraderId  uint        `json:"traderId"  ` // 交易员id
	Amount    uint64      `json:"amount"    ` //
	Status    uint        `json:"status"    ` // 可用0，不可用1，待更换2
	InitOrder uint        `json:"initOrder" ` // 绑定是否初始化仓位
	CreatedAt *gtime.Time `json:"createdAt" ` // 创建时间
	UpdatedAt *gtime.Time `json:"updatedAt" ` // 更新时间
	Num       float64     `json:"num"       ` //
}
