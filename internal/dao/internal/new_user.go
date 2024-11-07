// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// NewUserDao is the data access object for table new_user.
type NewUserDao struct {
	table   string         // table is the underlying table name of the DAO.
	group   string         // group is the database configuration group name of current DAO.
	columns NewUserColumns // columns contains all the column names of Table for convenient usage.
}

// NewUserColumns defines and stores column names for table new_user.
type NewUserColumns struct {
	Id                  string // 用户id
	Address             string // 用户address
	ApiStatus           string // api的可用状态：不可用2
	ApiKey              string // 用户币安apikey
	ApiSecret           string // 用户币安apisecret
	BindTraderStatus    string // 绑定trader状态：0未绑定，1绑定
	BindTraderStatusTfi string //
	UseNewSystem        string //
	IsDai               string //
	CreatedAt           string //
	UpdatedAt           string //
	BinanceId           string //
	NeedInit            string //
	Num                 string //
	ReInit              string //
}

// newUserColumns holds the columns for table new_user.
var newUserColumns = NewUserColumns{
	Id:                  "id",
	Address:             "address",
	ApiStatus:           "api_status",
	ApiKey:              "api_key",
	ApiSecret:           "api_secret",
	BindTraderStatus:    "bind_trader_status",
	BindTraderStatusTfi: "bind_trader_status_tfi",
	UseNewSystem:        "use_new_system",
	IsDai:               "is_dai",
	CreatedAt:           "created_at",
	UpdatedAt:           "updated_at",
	BinanceId:           "binance_id",
	NeedInit:            "need_init",
	Num:                 "num",
	ReInit:              "re_init",
}

// NewNewUserDao creates and returns a new DAO object for table data access.
func NewNewUserDao() *NewUserDao {
	return &NewUserDao{
		group:   "default",
		table:   "new_user",
		columns: newUserColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *NewUserDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *NewUserDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *NewUserDao) Columns() NewUserColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *NewUserDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *NewUserDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *NewUserDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
