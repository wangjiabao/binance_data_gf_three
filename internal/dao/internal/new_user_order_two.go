// ==========================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// ==========================================================================

package internal

import (
	"context"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// NewUserOrderTwoDao is the data access object for table new_user_order_two.
type NewUserOrderTwoDao struct {
	table   string                 // table is the underlying table name of the DAO.
	group   string                 // group is the database configuration group name of current DAO.
	columns NewUserOrderTwoColumns // columns contains all the column names of Table for convenient usage.
}

// NewUserOrderTwoColumns defines and stores column names for table new_user_order_two.
type NewUserOrderTwoColumns struct {
	Id            string // 主键id
	UserId        string //
	TraderId      string //
	ClientOrderId string //
	OrderId       string // binance系统订单号
	Symbol        string //
	Side          string // 买卖："SELL","BUY"
	PositionSide  string // 方向: 多"LONG",空"SHORT"
	Quantity      string // 下单数量
	Price         string // 价格
	TraderQty     string // 交易员下单数量
	OrderType     string // 类型：默认MARKET市价
	ClosePosition string // 是否条件全平仓
	CumQuote      string // 成交金额，biance返回真实的市价交易的成交金额
	ExecutedQty   string // 成交，biance返回真实的市价交易的数量
	AvgPrice      string // 平均价格，biance返回真实的市价交易价格
	HandleStatus  string //
	Status        string //
	TimeInForce   string //
	CreatedAt     string //
	UpdatedAt     string //
}

// newUserOrderTwoColumns holds the columns for table new_user_order_two.
var newUserOrderTwoColumns = NewUserOrderTwoColumns{
	Id:            "id",
	UserId:        "user_id",
	TraderId:      "trader_id",
	ClientOrderId: "client_order_id",
	OrderId:       "order_id",
	Symbol:        "symbol",
	Side:          "side",
	PositionSide:  "position_side",
	Quantity:      "quantity",
	Price:         "price",
	TraderQty:     "trader_qty",
	OrderType:     "order_type",
	ClosePosition: "close_position",
	CumQuote:      "cum_quote",
	ExecutedQty:   "executed_qty",
	AvgPrice:      "avg_price",
	HandleStatus:  "handle_status",
	Status:        "status",
	TimeInForce:   "time_in_force",
	CreatedAt:     "created_at",
	UpdatedAt:     "updated_at",
}

// NewNewUserOrderTwoDao creates and returns a new DAO object for table data access.
func NewNewUserOrderTwoDao() *NewUserOrderTwoDao {
	return &NewUserOrderTwoDao{
		group:   "default",
		table:   "new_user_order_two",
		columns: newUserOrderTwoColumns,
	}
}

// DB retrieves and returns the underlying raw database management object of current DAO.
func (dao *NewUserOrderTwoDao) DB() gdb.DB {
	return g.DB(dao.group)
}

// Table returns the table name of current dao.
func (dao *NewUserOrderTwoDao) Table() string {
	return dao.table
}

// Columns returns all column names of current dao.
func (dao *NewUserOrderTwoDao) Columns() NewUserOrderTwoColumns {
	return dao.columns
}

// Group returns the configuration group name of database of current dao.
func (dao *NewUserOrderTwoDao) Group() string {
	return dao.group
}

// Ctx creates and returns the Model for current DAO, It automatically sets the context for current operation.
func (dao *NewUserOrderTwoDao) Ctx(ctx context.Context) *gdb.Model {
	return dao.DB().Model(dao.table).Safe().Ctx(ctx)
}

// Transaction wraps the transaction logic using function f.
// It rollbacks the transaction and returns the error from function f if it returns non-nil error.
// It commits the transaction and returns nil if function f returns nil.
//
// Note that, you should not Commit or Rollback the transaction in function f
// as it is automatically handled by this function.
func (dao *NewUserOrderTwoDao) Transaction(ctx context.Context, f func(ctx context.Context, tx gdb.TX) error) (err error) {
	return dao.Ctx(ctx).Transaction(ctx, f)
}
