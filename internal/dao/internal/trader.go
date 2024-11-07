// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// TraderDao is the data access object for table trader.
type TraderDao struct {
	table   string        // table is the underlying table name of the DAO.
	group   string        // group is the database configuration group name of current DAO.
	columns TraderColumns // columns contains all the column names of Table for convenient usage.
}

// TraderColumns defines and stores column names for table trader.
type TraderColumns struct {
	Id          string //
	Name        string // 账号名称
	PortfolioId string // 投资组合ID
	IsOpen      string // 开启关闭 判断是否跟单
	BaseMoney   string // 带单保证金（自己）
	Lever       string // 杠杆倍数
	Area        string // 分区 1快速 2慢速
	Sort        string // 排序 数字小的在前面
	CreateTime  string //
	UpdateTime  string //
	Switch      string // 开关 判断是否抓取数据 如果开启为1 10s后is_open修改为1
	CloseTime   string // 关闭时间
	Level       string // 档位 1-7
	Amount      string //
	CreatedAt   string //
	UpdaatedAt  string //
}

// traderColumns holds the columns for table trader.
var traderColumns = TraderColumns{
	Id:          "id",
	Name:        "name",
	PortfolioId: "portfolioId",
	IsOpen:      "is_open",
	BaseMoney:   "base_money",
	Lever:       "lever",
	Area:        "area",
	Sort:        "sort",
	CreateTime:  "create_time",
	UpdateTime:  "update_time",
	Switch:      "switch",
	CloseTime:   "close_time",
	Level:       "level",
	Amount:      "amount",
	CreatedAt:   "created_at",
	UpdaatedAt:  "updaated_at",
}

// NewTraderDao creates and returns a new DAO object for table data access.
func NewTraderDao() *TraderDao {
	return &TraderDao{
		group:   "default",
		table:   "trader",
		columns: traderColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *TraderDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *TraderDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *TraderDao) Columns() TraderColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *TraderDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *TraderDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *TraderDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
