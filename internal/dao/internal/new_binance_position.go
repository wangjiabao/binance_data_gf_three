// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// NewBinancePositionDao is the data access object for table new_binance_position.
type NewBinancePositionDao struct {
	table   string                    // table is the underlying table name of the DAO.
	group   string                    // group is the database configuration group name of current DAO.
	columns NewBinancePositionColumns // columns contains all the column names of Table for convenient usage.
}

// NewBinancePositionColumns defines and stores column names for table new_binance_position.
type NewBinancePositionColumns struct {
	Id           string // 自增id
	Symbol       string //
	Side         string //
	PositionSide string //
	Qty          string //
	Status       string //
	CreatedAt    string //
	UpdatedAt    string //
}

// newBinancePositionColumns holds the columns for table new_binance_position.
var newBinancePositionColumns = NewBinancePositionColumns{
	Id:           "id",
	Symbol:       "symbol",
	Side:         "side",
	PositionSide: "position_side",
	Qty:          "qty",
	Status:       "status",
	CreatedAt:    "created_at",
	UpdatedAt:    "updated_at",
}

// NewNewBinancePositionDao creates and returns a new DAO object for table data access.
func NewNewBinancePositionDao() *NewBinancePositionDao {
	return &NewBinancePositionDao{
		group:   "default",
		table:   "new_binance_position",
		columns: newBinancePositionColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *NewBinancePositionDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *NewBinancePositionDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *NewBinancePositionDao) Columns() NewBinancePositionColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *NewBinancePositionDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *NewBinancePositionDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *NewBinancePositionDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
