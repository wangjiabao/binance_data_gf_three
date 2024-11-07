package main

import (
	"binance_data_gf/internal/cmd"
	_ "binance_data_gf/internal/logic"
	_ "github.com/gogf/gf/contrib/drivers/mysql/v2"
	"github.com/gogf/gf/v2/os/gctx"
)

func main() {
	// 添加命令
	var (
		err error
	)

	//err = cmd.Main.AddCommand(cmd.Trader)
	//if err != nil {
	//	panic(err)
	//}

	err = cmd.Main.AddCommand(cmd.TraderGui)
	if err != nil {
		panic(err)
	}

	cmd.Main.Run(gctx.GetInitCtx())
}
