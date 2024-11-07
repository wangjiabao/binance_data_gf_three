// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// TraderPositionDao is the data access object for table trader_position.
type TraderPositionDao struct {
	table   string                // table is the underlying table name of the DAO.
	group   string                // group is the database configuration group name of current DAO.
	columns TraderPositionColumns // columns contains all the column names of Table for convenient usage.
}

// TraderPositionColumns defines and stores column names for table trader_position.
type TraderPositionColumns struct {
	Id             string //
	Symbol         string //
	PositionSide   string //
	PositionAmount string //
	CreatedAt      string //
	UpdatedAt      string //
}

// traderPositionColumns holds the columns for table trader_position.
var traderPositionColumns = TraderPositionColumns{
	Id:             "id",
	Symbol:         "symbol",
	PositionSide:   "position_side",
	PositionAmount: "position_amount",
	CreatedAt:      "created_at",
	UpdatedAt:      "updated_at",
}

// NewTraderPositionDao creates and returns a new DAO object for table data access.
func NewTraderPositionDao() *TraderPositionDao {
	return &TraderPositionDao{
		group:   "default",
		table:   "trader_position",
		columns: traderPositionColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *TraderPositionDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *TraderPositionDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *TraderPositionDao) Columns() TraderPositionColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *TraderPositionDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *TraderPositionDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *TraderPositionDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
