// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// LhCoinSymbolDao is the data access object for table lh_coin_symbol.
type LhCoinSymbolDao struct {
	table   string              // table is the underlying table name of the DAO.
	group   string              // group is the database configuration group name of current DAO.
	columns LhCoinSymbolColumns // columns contains all the column names of Table for convenient usage.
}

// LhCoinSymbolColumns defines and stores column names for table lh_coin_symbol.
type LhCoinSymbolColumns struct {
	Id                string //
	Coin              string //
	Symbol            string //
	StartTime         string //
	EndTime           string //
	PricePrecision    string // 小数点精度
	QuantityPrecision string //
	IsOpen            string //
}

// lhCoinSymbolColumns holds the columns for table lh_coin_symbol.
var lhCoinSymbolColumns = LhCoinSymbolColumns{
	Id:                "id",
	Coin:              "coin",
	Symbol:            "symbol",
	StartTime:         "start_time",
	EndTime:           "end_time",
	PricePrecision:    "price_precision",
	QuantityPrecision: "quantity_precision",
	IsOpen:            "is_open",
}

// NewLhCoinSymbolDao creates and returns a new DAO object for table data access.
func NewLhCoinSymbolDao() *LhCoinSymbolDao {
	return &LhCoinSymbolDao{
		group:   "default",
		table:   "lh_coin_symbol",
		columns: lhCoinSymbolColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *LhCoinSymbolDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *LhCoinSymbolDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *LhCoinSymbolDao) Columns() LhCoinSymbolColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *LhCoinSymbolDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *LhCoinSymbolDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *LhCoinSymbolDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
