// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// NewUserInfoDao is the data access object for table new_user_info.
type NewUserInfoDao struct {
	table   string             // table is the underlying table name of the DAO.
	group   string             // group is the database configuration group name of current DAO.
	columns NewUserInfoColumns // columns contains all the column names of Table for convenient usage.
}

// NewUserInfoColumns defines and stores column names for table new_user_info.
type NewUserInfoColumns struct {
	Id        string //
	UserId    string //
	BId       string //
	BaseMoney string //
	CreatedAt string //
	UpdatedAt string //
}

// newUserInfoColumns holds the columns for table new_user_info.
var newUserInfoColumns = NewUserInfoColumns{
	Id:        "id",
	UserId:    "user_id",
	BId:       "b_id",
	BaseMoney: "base_money",
	CreatedAt: "created_at",
	UpdatedAt: "updated_at",
}

// NewNewUserInfoDao creates and returns a new DAO object for table data access.
func NewNewUserInfoDao() *NewUserInfoDao {
	return &NewUserInfoDao{
		group:   "default",
		table:   "new_user_info",
		columns: newUserInfoColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *NewUserInfoDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *NewUserInfoDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *NewUserInfoDao) Columns() NewUserInfoColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *NewUserInfoDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *NewUserInfoDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *NewUserInfoDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
