// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// NewBinancePositionHistory is the golang structure for table new_binance_position_history.
type NewBinancePositionHistory struct {
	Id        uint        `json:"id"        ` // 自增id
	Closed    uint64      `json:"closed"    ` //
	Opened    uint64      `json:"opened"    ` //
	Symbol    string      `json:"symbol"    ` //
	Side      string      `json:"side"      ` //
	Status    string      `json:"status"    ` //
	Qty       float64     `json:"qty"       ` //
	CreatedAt *gtime.Time `json:"createdAt" ` //
	UpdatedAt *gtime.Time `json:"updatedAt" ` //
}
