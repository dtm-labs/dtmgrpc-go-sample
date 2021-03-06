# dtmgrpc-go-sample
dtmgrpc的最简go使用示例

## 快速开始

### 安装运行dtm

参考[dtm安装运行](https://dtm.pub/guide/install.html)

### 启动示例

```
go run main.go
```

### 输出

可以从dtmgrpc-go-sample的日志里看到执行的顺序如下：

- TransOutTry
- TransInTry
- TransInConfirm
- TransOutConfirm

整个tcc事务执行成功

### 示例解读

核心代码如下，示例开启一个tcc全局事务，然后在事务内部注册并调用TransOut、TransIn分支，完成后返回，剩下的二阶段Confirm，会由DTM完成

``` GO
	// TccGlobalTransaction 开启一个TCC全局事务，第一个参数为dtm的地址，第二个参数是gid，第三个参数回调函数
	err := dtmcli.TccGlobalTransaction(dtm, gid, func(tcc *dtmcli.Tcc) (resp *resty.Response, rerr error) {
		// 调用TransOut分支，三个参数分别为post的body，tryUrl，confirmUrl，cancelUrl
		// res1 为try执行的结果
		resp, rerr = tcc.CallBranch(gin.H{"amount": 30}, tccBusi+"/TransOut", tccBusi+"/TransOutConfirm", tccBusi+"/TransOutCancel")
		if rerr != nil {
			return
		}
		// 调用TransIn分支
		resp, rerr = tcc.CallBranch(gin.H{"amount": 30}, tccBusi+"/TransIn", tccBusi+"/TransInConfirm", tccBusi+"/TransInCancel")
		if rerr != nil {
			return
		}
		// 返回后，tcc会把全局事务提交，DTM会调用个分支的Confirm
		return
	})
```

### 更多示例，详见[dtm-examples](https://github.com/dtm-labs/dtm-examples)
