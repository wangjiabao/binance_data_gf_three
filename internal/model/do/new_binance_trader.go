// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package do

import (
	"github.com/gogf/gf/v2/frame/g"
)

// NewBinanceTrader is the golang structure of table new_binance_trader for DAO operations like Where/Data.
type NewBinanceTrader struct {
	g.Meta    `orm:"table:new_binance_trader, do:true"`
	Id        interface{} // 主键自增id
	TraderNum interface{} //
	Status    interface{} // 状态
}
