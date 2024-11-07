// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// Trader is the golang structure for table trader.
type Trader struct {
	Id          uint        `json:"id"          ` //
	Name        string      `json:"name"        ` // 账号名称
	PortfolioId string      `json:"portfolioId" ` // 投资组合ID
	IsOpen      int         `json:"isOpen"      ` // 开启关闭 判断是否跟单
	BaseMoney   float64     `json:"baseMoney"   ` // 带单保证金（自己）
	Lever       float64     `json:"lever"       ` // 杠杆倍数
	Area        int         `json:"area"        ` // 分区 1快速 2慢速
	Sort        int         `json:"sort"        ` // 排序 数字小的在前面
	CreateTime  int         `json:"createTime"  ` //
	UpdateTime  int         `json:"updateTime"  ` //
	Switch      int         `json:"switch"      ` // 开关 判断是否抓取数据 如果开启为1 10s后is_open修改为1
	CloseTime   int         `json:"closeTime"   ` // 关闭时间
	Level       int         `json:"level"       ` // 档位 1-7
	Amount      int64       `json:"amount"      ` //
	CreatedAt   *gtime.Time `json:"createdAt"   ` //
	UpdaatedAt  *gtime.Time `json:"updaatedAt"  ` //
}
