// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// NewBinancePositionHistoryDao is the data access object for table new_binance_position_history.
type NewBinancePositionHistoryDao struct {
	table   string                           // table is the underlying table name of the DAO.
	group   string                           // group is the database configuration group name of current DAO.
	columns NewBinancePositionHistoryColumns // columns contains all the column names of Table for convenient usage.
}

// NewBinancePositionHistoryColumns defines and stores column names for table new_binance_position_history.
type NewBinancePositionHistoryColumns struct {
	Id        string // 自增id
	Closed    string //
	Opened    string //
	Symbol    string //
	Side      string //
	Status    string //
	Qty       string //
	CreatedAt string //
	UpdatedAt string //
}

// newBinancePositionHistoryColumns holds the columns for table new_binance_position_history.
var newBinancePositionHistoryColumns = NewBinancePositionHistoryColumns{
	Id:        "id",
	Closed:    "closed",
	Opened:    "opened",
	Symbol:    "symbol",
	Side:      "side",
	Status:    "status",
	Qty:       "qty",
	CreatedAt: "created_at",
	UpdatedAt: "updated_at",
}

// NewNewBinancePositionHistoryDao creates and returns a new DAO object for table data access.
func NewNewBinancePositionHistoryDao() *NewBinancePositionHistoryDao {
	return &NewBinancePositionHistoryDao{
		group:   "default",
		table:   "new_binance_position_history",
		columns: newBinancePositionHistoryColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *NewBinancePositionHistoryDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *NewBinancePositionHistoryDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *NewBinancePositionHistoryDao) Columns() NewBinancePositionHistoryColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *NewBinancePositionHistoryDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *NewBinancePositionHistoryDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *NewBinancePositionHistoryDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
