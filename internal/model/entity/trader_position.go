// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// TraderPosition is the golang structure for table trader_position.
type TraderPosition struct {
	Id             uint        `json:"id"             ` //
	Symbol         string      `json:"symbol"         ` //
	PositionSide   string      `json:"positionSide"   ` //
	PositionAmount float64     `json:"positionAmount" ` //
	CreatedAt      *gtime.Time `json:"createdAt"      ` //
	UpdatedAt      *gtime.Time `json:"updatedAt"      ` //
}
