// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

// LhCoinSymbol is the golang structure for table lh_coin_symbol.
type LhCoinSymbol struct {
	Id                uint   `json:"id"                ` //
	Coin              string `json:"coin"              ` //
	Symbol            string `json:"symbol"            ` //
	StartTime         int    `json:"startTime"         ` //
	EndTime           int    `json:"endTime"           ` //
	PricePrecision    int    `json:"pricePrecision"    ` // 小数点精度
	QuantityPrecision int    `json:"quantityPrecision" ` //
	IsOpen            int    `json:"isOpen"            ` //
}
