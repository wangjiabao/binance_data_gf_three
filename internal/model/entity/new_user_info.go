// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// NewUserInfo is the golang structure for table new_user_info.
type NewUserInfo struct {
	Id        uint        `json:"id"        ` //
	UserId    uint        `json:"userId"    ` //
	BId       int64       `json:"bId"       ` //
	BaseMoney float64     `json:"baseMoney" ` //
	CreatedAt *gtime.Time `json:"createdAt" ` //
	UpdatedAt *gtime.Time `json:"updatedAt" ` //
}
