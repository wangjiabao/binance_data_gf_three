// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

// Trader is the golang structure of table trader for DAO operations like Where/Data.
type Trader struct {
	g.Meta      `orm:"table:trader, do:true"`
	Id          interface{} //
	Name        interface{} // 账号名称
	PortfolioId interface{} // 投资组合ID
	IsOpen      interface{} // 开启关闭 判断是否跟单
	BaseMoney   interface{} // 带单保证金（自己）
	Lever       interface{} // 杠杆倍数
	Area        interface{} // 分区 1快速 2慢速
	Sort        interface{} // 排序 数字小的在前面
	CreateTime  interface{} //
	UpdateTime  interface{} //
	Switch      interface{} // 开关 判断是否抓取数据 如果开启为1 10s后is_open修改为1
	CloseTime   interface{} // 关闭时间
	Level       interface{} // 档位 1-7
	Amount      interface{} //
	CreatedAt   *gtime.Time //
	UpdaatedAt  *gtime.Time //
}
