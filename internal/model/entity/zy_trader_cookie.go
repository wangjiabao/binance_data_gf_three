// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// ZyTraderCookie is the golang structure for table zy_trader_cookie.
type ZyTraderCookie struct {
	Id         uint        `json:"id"         ` //
	Cookie     string      `json:"cookie"     ` //
	Token      string      `json:"token"      ` //
	TraderId   int         `json:"traderId"   ` //
	UpdateTime *gtime.Time `json:"updateTime" ` // 修正时间
	CloseTime  *gtime.Time `json:"closeTime"  ` // 关闭时间
	IsOpen     int         `json:"isOpen"     ` //
}
