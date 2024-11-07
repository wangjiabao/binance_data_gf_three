// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// KeyPosition is the golang structure of table key_position for DAO operations like Where/Data.
type KeyPosition struct {
	g.Meta    `orm:"table:key_position, do:true"`
	Id        interface{} //
	Key       interface{} //
	Amount    interface{} //
	CreatedAt *gtime.Time //
	UpdatedAt *gtime.Time //
}
