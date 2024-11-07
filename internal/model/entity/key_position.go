// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// KeyPosition is the golang structure for table key_position.
type KeyPosition struct {
	Id        uint        `json:"id"        ` //
	Key       string      `json:"key"       ` //
	Amount    float64     `json:"amount"    ` //
	CreatedAt *gtime.Time `json:"createdAt" ` //
	UpdatedAt *gtime.Time `json:"updatedAt" ` //
}
