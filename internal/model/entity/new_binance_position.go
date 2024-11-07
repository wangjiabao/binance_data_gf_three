// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// NewBinancePosition is the golang structure for table new_binance_position.
type NewBinancePosition struct {
	Id           uint        `json:"id"           ` // 自增id
	Symbol       string      `json:"symbol"       ` //
	Side         string      `json:"side"         ` //
	PositionSide string      `json:"positionSide" ` //
	Qty          float64     `json:"qty"          ` //
	Status       int         `json:"status"       ` //
	CreatedAt    *gtime.Time `json:"createdAt"    ` //
	UpdatedAt    *gtime.Time `json:"updatedAt"    ` //
}
