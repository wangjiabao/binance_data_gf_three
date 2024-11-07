// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// ZyTraderCookieDao is the data access object for table zy_trader_cookie.
type ZyTraderCookieDao struct {
	table   string                // table is the underlying table name of the DAO.
	group   string                // group is the database configuration group name of current DAO.
	columns ZyTraderCookieColumns // columns contains all the column names of Table for convenient usage.
}

// ZyTraderCookieColumns defines and stores column names for table zy_trader_cookie.
type ZyTraderCookieColumns struct {
	Id         string //
	Cookie     string //
	Token      string //
	TraderId   string //
	UpdateTime string // 修正时间
	CloseTime  string // 关闭时间
	IsOpen     string //
}

// zyTraderCookieColumns holds the columns for table zy_trader_cookie.
var zyTraderCookieColumns = ZyTraderCookieColumns{
	Id:         "id",
	Cookie:     "cookie",
	Token:      "token",
	TraderId:   "trader_id",
	UpdateTime: "update_time",
	CloseTime:  "close_time",
	IsOpen:     "is_open",
}

// NewZyTraderCookieDao creates and returns a new DAO object for table data access.
func NewZyTraderCookieDao() *ZyTraderCookieDao {
	return &ZyTraderCookieDao{
		group:   "default",
		table:   "zy_trader_cookie",
		columns: zyTraderCookieColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *ZyTraderCookieDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *ZyTraderCookieDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *ZyTraderCookieDao) Columns() ZyTraderCookieColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *ZyTraderCookieDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *ZyTraderCookieDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *ZyTraderCookieDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
