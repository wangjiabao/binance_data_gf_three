// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// NewTraderInfo is the golang structure for table new_trader_info.
type NewTraderInfo struct {
	Id        uint        `json:"id"        ` //
	TraderId  uint        `json:"traderId"  ` //
	BId       int64       `json:"bId"       ` //
	BaseMoney float64     `json:"baseMoney" ` //
	CreatedAt *gtime.Time `json:"createdAt" ` //
	UpdatedAt *gtime.Time `json:"updatedAt" ` //
}
