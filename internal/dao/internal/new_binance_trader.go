// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// NewBinanceTraderDao is the data access object for table new_binance_trader.
type NewBinanceTraderDao struct {
	table   string                  // table is the underlying table name of the DAO.
	group   string                  // group is the database configuration group name of current DAO.
	columns NewBinanceTraderColumns // columns contains all the column names of Table for convenient usage.
}

// NewBinanceTraderColumns defines and stores column names for table new_binance_trader.
type NewBinanceTraderColumns struct {
	Id        string // 主键自增id
	TraderNum string //
	Status    string // 状态
}

// newBinanceTraderColumns holds the columns for table new_binance_trader.
var newBinanceTraderColumns = NewBinanceTraderColumns{
	Id:        "id",
	TraderNum: "trader_num",
	Status:    "status",
}

// NewNewBinanceTraderDao creates and returns a new DAO object for table data access.
func NewNewBinanceTraderDao() *NewBinanceTraderDao {
	return &NewBinanceTraderDao{
		group:   "default",
		table:   "new_binance_trader",
		columns: newBinanceTraderColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *NewBinanceTraderDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *NewBinanceTraderDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *NewBinanceTraderDao) Columns() NewBinanceTraderColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *NewBinanceTraderDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *NewBinanceTraderDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *NewBinanceTraderDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
