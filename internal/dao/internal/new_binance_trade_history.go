// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// NewBinanceTradeHistoryDao is the data access object for table new_binance_trade_history.
type NewBinanceTradeHistoryDao struct {
	table   string                        // table is the underlying table name of the DAO.
	group   string                        // group is the database configuration group name of current DAO.
	columns NewBinanceTradeHistoryColumns // columns contains all the column names of Table for convenient usage.
}

// NewBinanceTradeHistoryColumns defines and stores column names for table new_binance_trade_history.
type NewBinanceTradeHistoryColumns struct {
	Id                  string // 自增id
	Time                string //
	Symbol              string //
	Side                string //
	PositionSide        string //
	Price               string //
	Fee                 string //
	FeeAsset            string //
	Quantity            string //
	QuantityAsset       string //
	RealizedProfit      string //
	RealizedProfitAsset string //
	BaseAsset           string //
	Qty                 string //
	ActiveBuy           string //
	CreatedAt           string //
	UpdatedAt           string //
}

// newBinanceTradeHistoryColumns holds the columns for table new_binance_trade_history.
var newBinanceTradeHistoryColumns = NewBinanceTradeHistoryColumns{
	Id:                  "id",
	Time:                "time",
	Symbol:              "symbol",
	Side:                "side",
	PositionSide:        "position_side",
	Price:               "price",
	Fee:                 "fee",
	FeeAsset:            "fee_asset",
	Quantity:            "quantity",
	QuantityAsset:       "quantity_asset",
	RealizedProfit:      "realized_profit",
	RealizedProfitAsset: "realized_profit_asset",
	BaseAsset:           "base_asset",
	Qty:                 "qty",
	ActiveBuy:           "active_buy",
	CreatedAt:           "created_at",
	UpdatedAt:           "updated_at",
}

// NewNewBinanceTradeHistoryDao creates and returns a new DAO object for table data access.
func NewNewBinanceTradeHistoryDao() *NewBinanceTradeHistoryDao {
	return &NewBinanceTradeHistoryDao{
		group:   "default",
		table:   "new_binance_trade_history",
		columns: newBinanceTradeHistoryColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *NewBinanceTradeHistoryDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *NewBinanceTradeHistoryDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *NewBinanceTradeHistoryDao) Columns() NewBinanceTradeHistoryColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *NewBinanceTradeHistoryDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *NewBinanceTradeHistoryDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *NewBinanceTradeHistoryDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
