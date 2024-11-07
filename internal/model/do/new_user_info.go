// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// NewUserInfo is the golang structure of table new_user_info for DAO operations like Where/Data.
type NewUserInfo struct {
	g.Meta    `orm:"table:new_user_info, do:true"`
	Id        interface{} //
	UserId    interface{} //
	BId       interface{} //
	BaseMoney interface{} //
	CreatedAt *gtime.Time //
	UpdatedAt *gtime.Time //
}
