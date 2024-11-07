// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// NewUser is the golang structure for table new_user.
type NewUser struct {
	Id                  uint        `json:"id"                  ` // 用户id
	Address             string      `json:"address"             ` // 用户address
	ApiStatus           uint        `json:"apiStatus"           ` // api的可用状态：不可用2
	ApiKey              string      `json:"apiKey"              ` // 用户币安apikey
	ApiSecret           string      `json:"apiSecret"           ` // 用户币安apisecret
	BindTraderStatus    uint        `json:"bindTraderStatus"    ` // 绑定trader状态：0未绑定，1绑定
	BindTraderStatusTfi uint        `json:"bindTraderStatusTfi" ` //
	UseNewSystem        int         `json:"useNewSystem"        ` //
	IsDai               int         `json:"isDai"               ` //
	CreatedAt           *gtime.Time `json:"createdAt"           ` //
	UpdatedAt           *gtime.Time `json:"updatedAt"           ` //
	BinanceId           int64       `json:"binanceId"           ` //
	NeedInit            int         `json:"needInit"            ` //
	Num                 float64     `json:"num"                 ` //
	ReInit              int         `json:"reInit"              ` //
}
