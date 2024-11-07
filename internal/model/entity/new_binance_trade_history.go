// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// NewBinanceTradeHistory is the golang structure for table new_binance_trade_history.
type NewBinanceTradeHistory struct {
	Id                  uint        `json:"id"                  ` // 自增id
	Time                uint64      `json:"time"                ` //
	Symbol              string      `json:"symbol"              ` //
	Side                string      `json:"side"                ` //
	PositionSide        string      `json:"positionSide"        ` //
	Price               float64     `json:"price"               ` //
	Fee                 float64     `json:"fee"                 ` //
	FeeAsset            string      `json:"feeAsset"            ` //
	Quantity            float64     `json:"quantity"            ` //
	QuantityAsset       string      `json:"quantityAsset"       ` //
	RealizedProfit      float64     `json:"realizedProfit"      ` //
	RealizedProfitAsset string      `json:"realizedProfitAsset" ` //
	BaseAsset           string      `json:"baseAsset"           ` //
	Qty                 float64     `json:"qty"                 ` //
	ActiveBuy           string      `json:"activeBuy"           ` //
	CreatedAt           *gtime.Time `json:"createdAt"           ` //
	UpdatedAt           *gtime.Time `json:"updatedAt"           ` //
}
