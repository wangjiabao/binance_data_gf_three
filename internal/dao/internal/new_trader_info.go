// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// NewTraderInfoDao is the data access object for table new_trader_info.
type NewTraderInfoDao struct {
	table   string               // table is the underlying table name of the DAO.
	group   string               // group is the database configuration group name of current DAO.
	columns NewTraderInfoColumns // columns contains all the column names of Table for convenient usage.
}

// NewTraderInfoColumns defines and stores column names for table new_trader_info.
type NewTraderInfoColumns struct {
	Id        string //
	TraderId  string //
	BId       string //
	BaseMoney string //
	CreatedAt string //
	UpdatedAt string //
}

// newTraderInfoColumns holds the columns for table new_trader_info.
var newTraderInfoColumns = NewTraderInfoColumns{
	Id:        "id",
	TraderId:  "trader_id",
	BId:       "b_id",
	BaseMoney: "base_money",
	CreatedAt: "created_at",
	UpdatedAt: "updated_at",
}

// NewNewTraderInfoDao creates and returns a new DAO object for table data access.
func NewNewTraderInfoDao() *NewTraderInfoDao {
	return &NewTraderInfoDao{
		group:   "default",
		table:   "new_trader_info",
		columns: newTraderInfoColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *NewTraderInfoDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *NewTraderInfoDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *NewTraderInfoDao) Columns() NewTraderInfoColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *NewTraderInfoDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *NewTraderInfoDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *NewTraderInfoDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
